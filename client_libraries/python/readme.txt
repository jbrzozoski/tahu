# To generate the base protobuf sparkplug_b Python library
protoc -I=../../sparkplug_b/ --python_out=./tahu ../../sparkplug_b/sparkplug_b.proto 
