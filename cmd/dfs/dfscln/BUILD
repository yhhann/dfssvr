package(default_visibility = ["//dfs:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_binary")

go_binary(
  name = "dfscln",
  srcs = glob(["*.go"], exclude = ["*_test.go"]),
  deps = [
    "//dfs/client:go_default_library",
    "//dfs/proto/transfer:go_default_library",
  ],
)
