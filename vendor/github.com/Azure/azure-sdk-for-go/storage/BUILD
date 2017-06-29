package(default_visibility = ["//visibility:public"])

licenses(["notice"])

load(
    "@io_bazel_rules_go//go:def.bzl",
    "go_library",
)

go_library(
    name = "go_default_library",
    srcs = [
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
        "appendblob.go",
=======
>>>>>>> fix godeps issue and change azure_file code due to api change
        "authorization.go",
        "blob.go",
        "blobsasuri.go",
        "blobserviceclient.go",
        "blockblob.go",
        "client.go",
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
        "container.go",
        "copyblob.go",
        "directory.go",
        "entity.go",
        "file.go",
        "fileserviceclient.go",
        "leaseblob.go",
        "message.go",
        "odata.go",
        "pageblob.go",
        "queue.go",
        "queueserviceclient.go",
        "share.go",
        "storagepolicy.go",
        "storageservice.go",
=======
        "directory.go",
        "file.go",
        "fileserviceclient.go",
        "queue.go",
        "share.go",
        "storagepolicy.go",
>>>>>>> fix godeps issue and change azure_file code due to api change
        "table.go",
        "table_batch.go",
        "tableserviceclient.go",
        "util.go",
        "version.go",
    ],
    tags = ["automanaged"],
    deps = [
        "//vendor/github.com/Azure/go-autorest/autorest:go_default_library",
        "//vendor/github.com/Azure/go-autorest/autorest/azure:go_default_library",
        "//vendor/github.com/satori/uuid:go_default_library",
    ],
)

filegroup(
    name = "package-srcs",
    srcs = glob(["**"]),
    tags = ["automanaged"],
    visibility = ["//visibility:private"],
)

filegroup(
    name = "all-srcs",
    srcs = [":package-srcs"],
    tags = ["automanaged"],
)
