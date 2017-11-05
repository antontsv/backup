package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	ini "gopkg.in/ini.v1"
)

var usage = func() {
	fmt.Fprintf(os.Stderr, "usage %s [-r] [-c config] file1 ... bucketName:[/path] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {

	providers := map[string]int{
		"amazon": 0,
		"google": 0,
	}

	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}

	provider := flag.String("p", strings.Join(names, ","), fmt.Sprintf("Cloud service provider names to use"))
	creds := flag.String("c", "backup.ini", "File with cloud account credentials")
	recursive := flag.Bool("r", false, "Recursively backup entire directories")
	flag.CommandLine.Usage = usage
	flag.Parse()

	num := flag.NArg()
	if num < 1 || flag.Arg(0) == "" {
		log.Fatalln("Missing source file/directory. This must be specified as a first parameter")
	} else if num < 2 {
		log.Fatalln("Missing target destination. This must be specified as a second parameter as follows 'bucketName:/some/optional/path'")
	}

	cfg, err := ini.InsensitiveLoad(*creds)
	if err != nil {
		log.Fatalf("Cannot read credentials file %s: %v\n", *creds, err)
	}

	for _, s := range cfg.Sections() {
		name := s.Name()
		if _, ok := providers[name]; ok {
			providers[name] = 1 // configured
		}
	}

	if *provider != "" {
		for _, p := range strings.Split(*provider, ",") {
			name := strings.TrimSpace(p)
			if _, ok := providers[name]; !ok {
				log.Fatalf("Selected cloud provider '%s' is not supported\n", name)
			}
			if providers[name] == 1 {
				providers[name] = 2 // configured & selected
			}
		}
	}

	var parts []string

	dest := strings.SplitN(flag.Arg(num-1), ":", 2)
	bucket := dest[0]

	if len(dest) > 1 {
		for _, part := range strings.Split(dest[1], "/") {
			if part != "" {
				parts = append(parts, part)
			}
		}
	}

	fmt.Println("Bucket:", bucket)
	fmt.Println("Destination path:", strings.Join(parts, "/"))

	for _, source := range flag.Args()[0 : num-1] {
		info, err := os.Stat(source)
		if err != nil {
			log.Fatalf("cannot open source: %s\n", source)
		}

		if info.IsDir() && !*recursive {
			log.Fatalf("%s is a directory, please use -r to make recursive backup\n", source)
		}
	}
}
