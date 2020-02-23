# ProtobufMessageFactory
This module tries to ease up the work with protobuf messages 
in python by allowing to automatically import and compile 
.proto files and access the messages by their name.
## Requirements
Protoc needs to be available on your system.
You can check this by executing:
```
protoc --version
```
If it is not installed download it here:
```
https://github.com/protocolbuffers/protobuf/releases
```
After the download you have to put the executable into your PATH.
