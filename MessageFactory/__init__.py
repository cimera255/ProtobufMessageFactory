import tempfile
import pathlib
from subprocess import call, STDOUT, DEVNULL
from shutil import copy2
from MessageFactory import Util
import sys
from contextlib import contextmanager

_PROTOBUF_SUFFIX = ".proto"

# Check if protoc is callable on this system
try:
    call(["protoc", "--version"], stdout=DEVNULL, stderr=STDOUT)
except FileNotFoundError:
    raise FileNotFoundError("\n\tFailed to execute a protoc command.\n"
                            "\tIs protoc located in the PATH of your system?\n"
                            "\tYou can download protoc under:\n"
                            "\thttps://github.com/protocolbuffers/protobuf/releases")


@contextmanager
def _temp_import(directory):
    import importlib.util
    from random import getrandbits
    from modulefinder import Module

    uid = getrandbits(128).to_bytes(16, "big").hex()

    m = Module(uid)
    sys.modules[uid] = m

    modules = list()

    # List to store the names of the imported modules so they can be deleted later
    module_names = list()
    module_names.append(uid)

    # List of elements in python_dir. It needs to be a list to be able to reschedule the import
    # of a file in case its import fails because a dependency is not imported at the moment.
    file_iterator = list(directory.iterdir())

    # Loop over the elements
    for element in file_iterator:
        # Check if the element is a file and a python module.
        if element.is_file() and element.suffix == ".py":
            # Create a module_name under which the module is imported
            module_name = uid + "." + element.parts[-1].replace(".py", "")

            # Actual import
            spec = importlib.util.spec_from_file_location(module_name, element)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module

            # Execute the module. This is needed for a complete import as it e.g.
            # executes the modules internal import statements (imports its dependencies).
            try:
                spec.loader.exec_module(module)
                # Store the module name in the list
                module_names.append(module_name)
                modules.append(module)
            except (ModuleNotFoundError, ImportError):
                # Catch errors caused by missing dependencies (which are maybe not imported at the moment)
                # These files get rescheduled at the end of the file list.
                # TODO(Joschua): At the moment there is no handling of infinite loops which can be
                #  caused by real missing modules or recursive imports.
                file_iterator.append(element)
                continue

    try:
        yield modules
    finally:
        # Code to release resource, e.g.:
        for module_name in module_names:
            sys.modules.pop(module_name)


class MessageFactory:
    """
    This class tries to ease up the work with protobuf messages in python.
    """
    MESSAGE_NAME = 0
    FILE_NAME = 1

    def __init__(self, work_dir=None, name_source=MESSAGE_NAME):
        """
        Initialize a new instance of MessageFactory.
        :param work_dir: The folder were all proto files get copied and compiled to
        when added to MessageFactory. If work_dir/python exists messages from contained python files are
        imported in the initialization process.
        --> If you are reusing the same folder each time the proto files only need to be added
        after the first initialization.
        :param name_source: Determines if the messages are available under their file name or message name
        after being added to the MessageFactory.
        FILE_NAME --> Messages will be available under the file name of the proto file (without suffix).
                      This causes issues if you use it with proto files which contain multiple messages.
        MESSAGES_NAME --> Messages will be available under the name of the message defined in the proto file.
                          This causes issues if you use the same message name in multiple proto files.
        """
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
            # It may be good to recompile the proto files in the proto_dir.
            # Also all files contained in this folder will already be compiled if they were added
            # by a previous run of this. --> NO recompile at this moment.
            # python_dir may not exist at this moment
            pass

        try:
            self.python_dir.mkdir()
        except FileExistsError:
            # Try to import messages from the folder as it already existed
            self._import_messages()

    def add_proto_files(self, files):
        """
        Base function for adding new proto files to the MessageFactory
        :param files: list of path to proto files. These will be copied to proto_dir, compiled to python_dir
        where their import statements will be corrected and then be searched for GeneratedProtoBufMessages.
        :return: None
        """
        new_files = list()

        for file in files:
            # This method overwrites without an error!
            file = pathlib.Path(copy2(file, self.proto_dir))
            new_files.append(file)

        for file in new_files:
            python_file = self._compile_proto_file(file)

            try:
                self._correct_imports(python_file)
            except FileNotFoundError:
                # File was not found. Maybe compilation failed.
                pass

        self._import_messages()

    def add_proto_dir(self, directory):
        """
        Add all proto files contained in a certain directory to the MessageFactory.
        This does not include subdirectories.
        :param directory: Path to a directory containing proto files to add.
        :return: None
        """
        directory = pathlib.Path(directory).absolute()
        files = list()

        for element in directory.iterdir():
            if element.is_file() and element.suffix == _PROTOBUF_SUFFIX:
                files.append(element)

        self.add_proto_files(files)

    def add_proto_file(self, file):
        """
        Same as add_proto_files but for a single file.
        :param file: path to a single proto file.
        :return: None
        """
        self.add_proto_files([file])

    def _compile_proto_file(self, file):
        """
        Compiles a single proto file with protoc (protoc has to be present on the system).
        Directory to import other proto messages is set to proto_dir.
        Directory for python output is set to python_dir.
        :param file: pathlib.Path to a proto file to be compiled.
        :return: Path to the created python file (ATTENTION: The path is although returned if the compilation failed!)
        """
        # Compile the file
        call(["protoc",
              "--proto_path", str(self.proto_dir),
              "--python_out", str(self.python_dir),
              str(file)])

        return self.python_dir.joinpath(file.parts[-1].replace(_PROTOBUF_SUFFIX, "_pb2.py"))

    @staticmethod
    def _correct_imports(python_file):
        """
        This function corrects the import statements in generated python files.
        This means changing the imports of other user messages from absolute imports
        from the root of the project to relative imports from the same package.
        :param python_file: pathlib.Path to a python file which imports should be corrected.
        :return: None
        """
        # Read in the python module as text
        data = python_file.read_text()

        # Separate the part with imports from google.protobuf from the custom imports
        [stay, fix] = data.split("# @@protoc_insertion_point(imports)")

        # Correct the import statements into relative imports
        fix = fix.replace("\nimport ", "\nfrom . import ")

        # Combine the parts
        data = stay + "# @@protoc_insertion_point(imports)" + fix

        # Write the corrected code back into the module file
        python_file.write_text(data)


    def _search_messages_in_modules(self, modules):
        from google.protobuf.pyext.cpp_message import GeneratedProtocolMessageType

        for module in modules:
            # Loop over the attributes of the module
            for name, value in module.__dict__.items():
                # Correct the name under which the message is stored in case name_source is set to FILE_NAME
                if self.name_source == self.FILE_NAME:
                    name = module.DESCRIPTOR.name.replace(_PROTOBUF_SUFFIX, "")

                # Check if the attribute is a message and store it if it is.
                if type(value) is GeneratedProtocolMessageType:
                    self.messages[name] = value

    def _import_messages(self):
        """
        Imports all messages from the modules located in python_dir.
        :return: None
        """
        with _temp_import(self.python_dir) as modules:
            self._search_messages_in_modules(modules)

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

    def get_message_dict(self, message_name):
        msg = self.get_message_prototype(message_name)

        return Util.message_to_dict(msg)

    def get_message_json(self, message_name):
        msg = self.get_message_prototype(message_name)

        return Util.message_to_json(msg)
