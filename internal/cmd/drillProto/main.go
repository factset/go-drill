package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/docopt/docopt-go"
	"github.com/google/go-github/v32/github"
)

const usage = `Drill Proto.

Usage:
	drillProto -h | --help
	drillProto download [-o PATH]
	drillProto fixup [-o PATH]
	drillProto gen [-o PATH] ROOTPATH
	drillProto runall [-o PATH] ROOTPATH

Arguments:
	ROOTPATH  location of the root output for the generated .go files

Options:
	-h --help           Show this screen.
	-o PATH --out PATH  .proto destination path [default: protobuf]`

func download(outdir string) {
	fmt.Println("Download .proto files from Apache Drill Git Repo")
	client := github.NewClient(nil)

	_, dircont, _, err := client.Repositories.GetContents(context.Background(), "apache", "drill", "protocol/src/main/protobuf", &github.RepositoryContentGetOptions{Ref: "master"})
	if err != nil {
		log.Fatal(err)
	}

	info, err := os.Stat(outdir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(outdir, os.ModePerm); err != nil {
			log.Fatal(err)
		}
	} else {
		if !info.IsDir() {
			log.Fatal("Path is a file, can't be used")
		}
	}

	for _, c := range dircont {
		path := filepath.Join(outdir, c.GetName())
		log.Printf("Downloading: %s to %s\n", c.GetName(), path)

		resp, err := http.Get(c.GetDownloadURL())
		if err != nil {
			log.Fatal(err)
		}

		defer resp.Body.Close()
		out, err := os.Create(path)
		if err != nil {
			log.Fatal(err)
		}
		defer out.Close()

		_, err = io.Copy(out, resp.Body)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func fixup(dirpath string) {
	fmt.Println("Update .proto files with go_package option")
	files, err := ioutil.ReadDir(dirpath)
	if err != nil {
		log.Fatal(err)
	}

	reGopkg := regexp.MustCompile(`option go_package = (".*");`)
	rePkg := regexp.MustCompile(`package (?P<package>.*);`)

	root := []string{"github.com/zeroshade/go-drill/internal/rpc/proto"}

	for _, f := range files {
		contents, err := ioutil.ReadFile(filepath.Join(dirpath, f.Name()))
		if err != nil {
			log.Fatal(err)
		}

		if reGopkg.Match(contents) {
			continue
		}

		submatches := rePkg.FindSubmatchIndex(contents)
		pieces := strings.Split(string(contents[submatches[2]:submatches[3]]), ".")
		pkgname := filepath.Join(append(root, pieces...)...)

		insert := fmt.Sprintf("option go_package = \"%s\";\n", pkgname)

		data := make([]byte, len(contents)+len(insert))
		copy(data, contents[:submatches[1]+2])
		copy(data[submatches[1]+2:], []byte(insert))
		copy(data[submatches[1]+2+len(insert):], contents[submatches[1]+2:])

		if err = ioutil.WriteFile(filepath.Join(dirpath, f.Name()), data, os.ModePerm); err != nil {
			log.Fatal(err)
		}
	}
}

func gen(dirpath, outpath string) {
	fmt.Println("Generate the .go files from the proto definitions")
	dirpath, _ = filepath.Abs(dirpath)
	outpath, _ = filepath.Abs(outpath)

	protos, err := filepath.Glob(dirpath + "/*.proto")
	if err != nil {
		log.Fatal(err)
	}

	args := []string{
		"--proto_path=" + dirpath,
		"--go_out=" + outpath,
	}
	args = append(args, protos...)

	cmd := exec.Command("protoc", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(string(out))
		log.Fatal(err)
	}
}

func main() {
	opts, err := docopt.ParseDoc(usage)
	if err != nil {
		panic(err)
	}
	var config struct {
		Help     bool   `docopt:"-h"`
		Dir      string `docopt:"--out"`
		Download bool
		Fixup    bool
		Gen      bool
		RunAll   bool   `docopt:"runall"`
		RootPath string `docopt:"ROOTPATH"`
	}

	opts.Bind(&config)

	if config.Download || config.RunAll {
		download(config.Dir)
	}

	if config.Fixup || config.RunAll {
		fixup(config.Dir)
	}

	if config.Gen || config.RunAll {
		gen(config.Dir, config.RootPath)
	}
}
