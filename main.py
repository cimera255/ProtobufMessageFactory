import tempfile
import pathlib
from subprocess import call
from shutil import copy2


class MessageFactory:
    MESSAGE_NAME = 0
    FILE_NAME = 1

    def __init__(self, work_dir=None, name_source=MESSAGE_NAME):
        self.messages = dict()
        self.name_source = name_source
        self.work_dir = pathlib.Path(tempfile.gettempdir() if work_dir is None else work_dir).absolute()

        if not self.work_dir.exists():
            raise NotADirectoryError("The directory does not exist.")

        self.proto_dir = self.work_dir.joinpath("proto")
        self.python_dir = self.work_dir.joinpath("python")

        try:
            self.proto_dir.mkdir()
        except FileExistsError:
            # TODO(Joschua): Implement some error handling
            pass

        try:
            self.python_dir.mkdir()
        except FileExistsError:
            # TODO(Joschua): Implement some error handling
            pass

    def add_proto_dir(self, directory):
        directory = pathlib.Path(directory).absolute()

        for element in directory.iterdir():
            if element.is_file() and element.suffix == ".proto":
                self.add_proto_file(element, _import=False)

        self._import_messages()

    def add_proto_file(self, file, _import=True):
        # This method overwrites without an error!
        file = pathlib.Path(copy2(file, self.proto_dir))

        python_file = self._compile_proto_file(file)
        
        try:
            self._correct_imports(python_file)

            if _import:
                self._import_messages()
        except:
            # File was not found. Maybe compilation failed. 
            pass

    def _compile_proto_file(self, file):
        call(["protoc",
              "--proto_path", str(self.proto_dir),
              "--python_out", str(self.python_dir),
              str(file)])

        return self.python_dir.joinpath(file.parts[-1].replace(".proto", "_pb2.py"))

    @staticmethod
    def _correct_imports(python_file):
        data = python_file.read_text()

        [stay, fix] = data.split("# @@protoc_insertion_point(imports)")

        fix = fix.replace("\nimport ", "\nfrom . import ")

        data = stay + "# @@protoc_insertion_point(imports)" + fix

        python_file.write_text(data)

    def _import_messages(self):
        import importlib.util
        import sys
        from google.protobuf.pyext.cpp_message import GeneratedProtocolMessageType
        module_names = list()

        file_iterator = list(self.python_dir.iterdir())

        for element in file_iterator:
            if element.is_file() and element.suffix == ".py":
                module_name = "proto." + element.parts[-1].replace(".py", "")

                spec = importlib.util.spec_from_file_location(module_name, element)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module

                try:
                    spec.loader.exec_module(module)
                except ModuleNotFoundError as e:
                    file_iterator.append(element)
                    continue

                for [name, value] in module.__dict__.items():
                    if self.name_source == self.FILE_NAME:
                        name = module.DESCRIPTOR.name.replace(".proto", "") 
                    if type(value) is GeneratedProtocolMessageType:
                        self.messages[name] = value

                module_names.append(module_name)

        for module_name in module_names:
            sys.modules.pop(module_name)

    def get_message_class(self, message_name):
        return self.messages.get(message_name)

    def get_message_prototype(self, message_name):
        message_class = self.get_message_class(message_name)

        try:
            prototype = message_class() 
        except:
            return None

        return prototype 

