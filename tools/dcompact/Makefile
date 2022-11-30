
INCS := \
  -I../../3rdparty/etcd-cpp-apiv3 \
  -I../../3rdparty/etcd-cpp-apiv3/build/proto/gen/proto \
  -I../../../../../vcpkg/packages/cpprestsdk_x64-linux/include \
  -I../../../../../vcpkg/packages/grpc_x64-linux/include \
  -I../../../../../vcpkg/packages/protobuf_x64-linux/include

ifeq (${TOPLING_DCOMPACT_USE_ETCD},1)
LDFLAGS := -L../../3rdparty/etcd-cpp-apiv3/build/src -letcd-cpp-api -lstdc++fs
endif

include exe-common.mk

