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

    def add_proto_files(self, files):
        new_files = list()

        for file in files:
            # This method overwrites without an error!
            file = pathlib.Path(copy2(file, self.proto_dir))
            new_files.append(file)

        for file in new_files:
            python_file = self._compile_proto_file(file)

            try:
                self._correct_imports(python_file)
            except:
                # File was not found. Maybe compilation failed.
                pass

            self._import_messages()

    def add_proto_dir(self, directory):
        directory = pathlib.Path(directory).absolute()
        files = list()

        for element in directory.iterdir():
            if element.is_file() and element.suffix == ".proto":
                files.append(element)

        self.add_proto_files(files)

    def add_proto_file(self, file):
        self.add_proto_files([file])

    def _compile_proto_file(self, file):
        # Compile the file
        call(["protoc",
              "--proto_path", str(self.proto_dir),
              "--python_out", str(self.python_dir),
              str(file)])

        return self.python_dir.joinpath(file.parts[-1].replace(".proto", "_pb2.py"))

    @staticmethod
    def _correct_imports(python_file):
        # Read in the python module as text
        data = python_file.read_text()

        # Separate the part with imports from google.protobuf from the custom imports
        # TODO(Joschua): If there is a python file which differs from a standard
        #  compiled proto file this might throw an exception.
        [stay, fix] = data.split("# @@protoc_insertion_point(imports)")

        # Correct the import statements into relative imports
        fix = fix.replace("\nimport ", "\nfrom . import ")

        # Combine the parts
        data = stay + "# @@protoc_insertion_point(imports)" + fix

        # Write the corrected code back into the module file
        python_file.write_text(data)

    def _import_messages(self):
        """
        Imports all messages from the modules located in python_dir.
        :return: None
        """
        import importlib.util
        import sys
        from google.protobuf.pyext.cpp_message import GeneratedProtocolMessageType
        # List to store the names of the imported modules so they can be deleted later
        module_names = list()

        # List of elements in python_dir. It needs to be a list to be able to reschedule the import
        # of a file in case its import fails because a dependency is not imported at the moment.
        file_iterator = list(self.python_dir.iterdir())

        # Loop over the elements
        for element in file_iterator:
            # Check if the element is a file and a python module.
            if element.is_file() and element.suffix == ".py":
                # Create a module_name under which the module is imported
                # TODO(Joschua): Make sure the module name is really unique.
                #  This could be done by setting the prefix to a random string.
                module_name = "proto." + element.parts[-1].replace(".py", "")

                # Actual import
                spec = importlib.util.spec_from_file_location(module_name, element)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module

                # Execute the module. This is needed for a complete import as it e.g.
                # executes the modules internal import statements (imports its dependencies).
                try:
                    spec.loader.exec_module(module)
                except (ModuleNotFoundError, ImportError) as e:
                    # Catch errors caused by missing dependencies (which are maybe not imported at the moment)
                    # These files get rescheduled at the end of the file list.
                    # TODO(Joschua): At the moment there is no handling of infinite loops which can be
                    #  caused by real missing modules or recursive imports.
                    file_iterator.append(element)
                    continue

                # Loop over the attributes of the module
                for [name, value] in module.__dict__.items():
                    # Correct the name under which the message is stored in case name_source is set to FILE_NAME
                    if self.name_source == self.FILE_NAME:
                        name = module.DESCRIPTOR.name.replace(".proto", "")

                    # Check if the attribute is a message and store it if it is.
                    if type(value) is GeneratedProtocolMessageType:
                        self.messages[name] = value

                # Store the module name in the list
                module_names.append(module_name)

        # Delete all imported modules from sys.modules
        for module_name in module_names:
            sys.modules.pop(module_name)

    def get_message_class(self, message_name):
        """
        Searches the added messages for one with a matching massage_name.
        :param message_name: name of the message you want to get the class for.
        :return: massage_class or None
        """
        return self.messages.get(message_name)

    def get_message_prototype(self, message_name):
        """
        Gives you an initialized instance of a message with the name message_name
        :param message_name: Name of the message you are searching for
        :return: Instance of message_class or None
        """
        message_class = self.get_message_class(message_name)

        try:
            # Initialize a instance of the message_class
            prototype = message_class() 
        except TypeError:
            # Catch error caused if no matching message_class was found
            return None

        return prototype 

