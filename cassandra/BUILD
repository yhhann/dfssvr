package(default_visibility = ["//dfs:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = glob(
        ["*.go"],
        exclude = [
            "*_test.go",
            "mock_*.go",
        ],
    ),
    deps = [
        "//dfs/meta:go_default_library",
        "//dfs/util:go_default_library",
        "//third-party-go/vendor/github.com/gocql/gocql:go_default_library",
        "//third-party-go/vendor/github.com/golang/glog:go_default_library",
        "//third-party-go/vendor/github.com/relops/cqlr:go_default_library",
        "//third-party-go/vendor/gopkg.in/mgo.v2/bson:go_default_library",
    ],
)
