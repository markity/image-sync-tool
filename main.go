package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/tencentyun/cos-go-sdk-v5"
)

var BucketURL = "xxxxx-1500000000"
var AP = "ap-chongqing"
var SercertID = "xxxxxxxxxxxxxxxxxxxxxx"
var SecretKey = "xxxxxxxxxxxxxxxxxxxxxx"

func main() {
	argsLen := len(os.Args)
	if argsLen < 2 {
		fmt.Printf("usage: %s <sync/get> <more_arguments>...", os.Args[0])
		return
	}

	op := os.Args[1]
	path := os.Args[2]
	var filePath string

	switch op {
	case "sync":
		if argsLen != 3 {
			fmt.Printf("usage: %s sync <path>", os.Args[0])
			return
		}
	case "get":
		if argsLen != 4 {
			fmt.Printf("usage: %s sync <path> <file_path>", os.Args[0])
			return
		}
		filePath = os.Args[3]
	}

	stat, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("<path> not exists")
			return
		}
		if !stat.IsDir() {
			fmt.Println("<path> not a dir")
			return
		}
		panic(err)
	}

	if op == "get" {
		if !filepath.HasPrefix(filePath, path) {
			fmt.Println("<filepath> must has prefix <path>")
			return
		}
	}

	bucketURL, _ := url.Parse(fmt.Sprintf("https://%s.cos.%s.myqcloud.com", BucketURL, AP))
	serviceURL, _ := url.Parse(fmt.Sprintf("https://cos.%s.myqcloud.com", AP))

	client := cos.NewClient(&cos.BaseURL{
		BucketURL:  bucketURL,
		ServiceURL: serviceURL,
	}, &http.Client{
		Transport: &cos.AuthorizationTransport{
			SecretID:  SercertID,
			SecretKey: SecretKey,
		},
	})

	if op == "sync" {
		systemFiles := make(map[string]string)

		filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
			if d.IsDir() {
				return nil
			}

			if err != nil {
				panic(err)
			}

			relPath, err := filepath.Rel(path, p)
			if err != nil {
				panic(err)
			}

			f, err := os.ReadFile(p)
			if err != nil {
				panic(err)
			}
			m := md5.New()
			m.Write(f)
			systemFiles[relPath] = fmt.Sprintf("%x", m.Sum(nil))
			return nil
		})

		result, resp, err := client.Bucket.Get(context.Background(), &cos.BucketGetOptions{
			MaxKeys: 1000,
		})
		if err != nil {
			panic(err)
		}
		resp.Body.Close()

		var alreadyHaveItems = make(map[string]string)

		listAll := len(result.Contents) < 1000
		for {
			for _, v := range result.Contents {
				alreadyHaveItems[v.Key] = v.ETag
			}

			if listAll {
				break
			}

			marker := result.Marker
			result, resp, err = client.Bucket.Get(context.Background(), &cos.BucketGetOptions{
				MaxKeys: 1000,
				Marker:  marker,
			})
			if err != nil {
				panic(err)
			}
			resp.Body.Close()
			if len(result.Contents) < 1000 {
				listAll = true
			}
		}

		// 做上传
		// 1. 删除远端有的, 但是这里没有的
		for k := range alreadyHaveItems {
			if _, ok := systemFiles[k]; !ok {
				fmt.Println("delete", k)
				resp, err := client.Object.Delete(context.Background(), k, &cos.ObjectDeleteOptions{})
				if err != nil {
					panic(err)
				}
				resp.Body.Close()
			}
		}

		// 2. 更新md5不一致的, 活不存在的
		for k, v := range systemFiles {
			av, ok := alreadyHaveItems[k]
			if fmt.Sprintf(`"%s"`, v) != av || !ok {
				fmt.Println("update", k)
				data, err := os.ReadFile(filepath.Join(path, k))
				if err != nil {
					panic(err)
				}
				buf := bytes.NewBuffer(data)
				resp, err := client.Object.Put(context.Background(), k, buf, &cos.ObjectPutOptions{ACLHeaderOptions: &cos.ACLHeaderOptions{
					XCosACL: "public-read",
				}})
				if err != nil {
					panic(err)
				}
				resp.Body.Close()
			}
		}

		fmt.Println("ok")
	} else if op == "get" {
		filePath := os.Args[3]
		relPath, err := filepath.Rel(path, filePath)
		if err != nil {
			panic(err)
		}

		s, err := os.Stat(filePath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				fmt.Println("not exists")
				return
			}
			if s.IsDir() {
				fmt.Println("cannot be a dir")
				return
			}
			panic(err)
		}

		url := client.Object.GetObjectURL(relPath)
		fmt.Println(url)
	}

}
