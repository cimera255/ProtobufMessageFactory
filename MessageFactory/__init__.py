import tempfile
import pathlib
from subprocess import call, STDOUT, DEVNULL
from shutil import copy2
from MessageFactory import Util
import sys
from contextlib import contextmanager
import logging

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
def _temp_import(directory, log_level=logging.WARNING):
    import importlib.util
    from random import getrandbits
    from modulefinder import Module

    logger = logging.getLogger("Temp-Import")
    logger.setLevel(log_level)

    logger.debug(f'Starting temporary import from "{directory}".')

    uid = getrandbits(128).to_bytes(16, "big").hex()
    logger.debug(f'Modules are imported under "{uid}"')

    m = Module(uid)
    sys.modules[uid] = m

    modules = list()

    # List to store the names of the imported modules so they can be deleted later
    module_names = list()
    module_names.append(uid)

    # List of elements in python_dir. It needs to be a list to be able to reschedule the import
    # of a file in case its import fails because a dependency is not imported at the moment.
    file_iterator = list(directory.iterdir())

    # TODO(Joschua): This needs to be discussed.
    #  Theoretically the maximal amount of import attempts needed should be equal
    #  to the total number of imports.
    #  This is true under the assumption that the first import is dependent on
    #  the second which depends on the third and so on.
    #  After this number of attempts it can be assumed that there is an error
    #  preventing the import which can not be solved by importing other modules.
    #  Practically it may be sufficient to use a smaller number.
    #  Decision should be made based on performance impact of the higher number
    #  in contrast to a smaller.
    max_retries = len(file_iterator)

    # Loop over the elements
    for element in file_iterator:
        # Check if the element is a file and a python module.
        if element.is_file() and element.suffix == ".py":
            logger.debug(f'Importing "{element}"')
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
                modules.append(module)
                # Store the module name in the list
                module_names.append(module_name)
            except (ModuleNotFoundError, ImportError):
                # Catch errors caused by missing dependencies (which are maybe not imported at the moment)
                # These files get rescheduled at the end of the file list.
                retries = file_iterator.count(element)
                logger.warning(f'{retries}/{max_retries} import attempt failed: "{element}"')

                if retries >= max_retries:
                    logger.error(f'Maximum number of import attempts reached for "{element}".'
                                 f'\nContent will not be imported!')
                    # Store the module name in the list
                    module_names.append(module_name)
                else:
                    file_iterator.append(element)
                continue

            logger.debug(f'Successfully imported "{element}"')

    try:
        # Entry point for custom function. Serving a list of the imported modules.
        logger.debug('Entering user part of the contextmanager.')
        yield modules
    finally:
        # Code to release resources:
        logger.debug("Releasing resources.")
        for module_name in module_names:
            sys.modules.pop(module_name)


class MessageFactory:
    """
    This class tries to ease up the dynamic work with protobuf messages in python.
    """
    # Messages can be accessed under the message name defined in their .proto file
    MESSAGE_NAME = 0
    # Messages can be accessed under the name of the .proto file they are contained in (without suffix)
    FILE_NAME = 1
    # Message can be accessed under <file_name>.<message_name> (File name without suffix)
    BOTH = 2

    def __init__(self, work_dir=None, name_source=MESSAGE_NAME):
        """
        Initialize a new instance of MessageFactory.
        :param work_dir: The folder were all proto files get copied and compiled to
        when added to MessageFactory. If work_dir/python exists messages from contained python files are
        imported in the initialization process.
        --> If you are reusing the same folder each time the proto files only need to be added
        after the first initialization.
        :param name_source: Determines if the messages are available under their file name, message name
        or a combination of both after being added to the MessageFactory.
        FILE_NAME --> Messages will be available under the file name of the proto file (without suffix).
                      This causes issues (collision) if you use it with proto files which contain multiple messages.
        MESSAGES_NAME --> Messages will be available under the name of the message defined in the proto file.
                          This causes issues (collision) if you use the same message name in multiple proto files.
        BOTH --> Messages will be available under <file_name>.<message_name> (file name without suffix).
                 This does not cause collision issues but makes the names much longer and more complex.
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
        """
        Searches modules for GeneratedProtocolMessageType and added them to the internal dict.
        :param modules: List of modules to search in.
        :return: None
        """
        from google.protobuf.pyext.cpp_message import GeneratedProtocolMessageType

        for module in modules:
            # Loop over the attributes of the module
            for name, value in module.__dict__.items():
                # Correct the name under which the message is stored in case name_source is set to FILE_NAME
                if self.name_source == self.FILE_NAME:
                    name = module.DESCRIPTOR.name.replace(_PROTOBUF_SUFFIX, "")
                elif self.name_source == self.BOTH:
                    name = f'{module.DESCRIPTOR.name.replace(_PROTOBUF_SUFFIX, "")}.{name}'

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
        """
        Gives you a dict representation of the message with the name message_name.
        All fields of the dict are prefilled with default values.
        :param message_name: Name of the message you want a dict representation for.
        :return: dict
        """
        # Get a message instance. This may return None if message_name is unknown
        msg = self.get_message_prototype(message_name)

        try:
            return Util.message_to_dict(msg)
        except AttributeError:
            # Error handling if message_name is unknown
            return None

    def get_message_json(self, message_name):
        """
        Gives you a json representation of the message with the name message_name.
        All fields of the json are prefilled with default values.
        :param message_name: Name of the message you want a json representation for.
        :return: str (json)
        """
        # Get a message instance. This may return None if message_name is unknown
        msg = self.get_message_prototype(message_name)

        try:
            return Util.message_to_json(msg)
        except AttributeError:
            # Error handling if message_name is unknown
            return None
