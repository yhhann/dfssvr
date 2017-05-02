package(default_visibility = ["//dfs:__subpackages__"])

load("@bazel_tools//tools/build_defs/docker:docker.bzl", "docker_build")
load("@io_bazel_rules_go//go:def.bzl", "go_binary")

go_binary(
    name = "dfssvr",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    linkstamp = "jingoal.com/letsgo/version",
    x_defs = {
        "main.buildTime": "2016/06/30_00:00",
    },
    deps = [
        "//dfs/instrument:go_default_library",
        "//dfs/proto/discovery:go_default_library",
        "//dfs/proto/transfer:go_default_library",
        "//dfs/server:go_default_library",
        "//letsgo:go_default_library",
        "//letsgo/grpc:go_default_library",
        "//third-party-go/vendor/github.com/golang/glog:go_default_library",
        "//third-party-go/vendor/google.golang.org/grpc:go_default_library",
    ],
)

docker_build(
    name = "dfssvr-img",
    base = "//:base",
    cmd = "/dfs/bin/start.sh",
    directory = "/dfs/bin",
    files = [
        "start.sh",
        ":dfssvr",
    ],
    workdir = "/dfs",
)
