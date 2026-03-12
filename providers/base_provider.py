from abc import ABC, abstractmethod
from typing import Union, List
 import logging

import memoization
memoization.suppress_warnings()
from memoization import cached, CachingAlgorithmFlag
from tenacity import (
    retry,
    stop_after_delay,
    stop_after_attempt,
    wait_random_exponential,
)

try:
    from .utils import hash_input, pop_half_dict
except ImportError:
    from utils import hash_input, pop_half_dict

logger = logging.getLogger(__name__)


# Cache the fail prompt to avoid running translation again for subsequent calls
GLOBAL_CACHE_FAIL_PROMPT = {}
GLOBAL_MAX_LIST_RETRIES = 20 # Global max retries for list translation
GLOBAL_MAX_STRING_RETRIES = 10 # Global max retries for string translation


class Provider(ABC):
    """
    Base Provider that must be inherited by all Provider class, implement your own provider by inheriting this class
    """
    @abstractmethod
    def __init__(self):
        self.translator = None

    @abstractmethod
    def _do_translate(self, input_data: Union[str, List[str]], src: str, dest: str, fail_translation_code:str = "P1OP1_F", **kwargs) -> Union[str, List[str]]:
        """
        Perform translation of input data from source language to destination language.

        Args:
            input_data (Union[str, List[str]]): The input data to be translated. It can be a single string or a list of strings.
            src (str): The source language code.
            dest (str): The destination language code.
            fail_translation_code (str, optional): The code to be returned when translation fails. Defaults to "P1OP1_F".
            **kwargs: Additional keyword arguments for translation.

        Returns:
            Union[str, List[str]]: The translated output data. It can be a single string or a list of strings.
        """
        raise NotImplemented(" The function _do_translate has not been implemented.")

    def __get_hashable_key(self, input_data: Union[str, List[str]], src: str, dest: str, fail_translation_code: str="P1OP1_F") -> str:
        """
        Generate a hashable key for the input data, source language, destination language, and fail_translation_code.

        Args:
            input_data (Union[str, List[str]]): The input data to be translated. It can be a single string or a list of strings.
            src (str): The source language code.
            dest (str): The destination language code.
            fail_translation_code (str, optional): The code to be returned when translation fails. Defaults to "P1OP1_F".

        Returns:
            str: The hashable key for the input data, source language, destination language, and fail_translation_code.
        """

        return f"{src}-{dest}-{hash_input(input_data, hash=False)}-{fail_translation_code}"
    
    @cached(max_size=5000, thread_safe=False, custom_key_maker=__get_hashable_key, algorithm=CachingAlgorithmFlag.LRU, ttl=7200)
    @retry(stop=(stop_after_attempt(max(GLOBAL_MAX_LIST_RETRIES, GLOBAL_MAX_STRING_RETRIES)) | stop_after_delay(180)), wait=wait_random_exponential(multiplier=1, max=30), reraise=True)
    def translate(self, input_data: Union[str, List[str]], src: str, dest: str, fail_translation_code: str="P1OP1_F") -> Union[str, List[str]]:
        """
        Translates the input data from the source language to the destination language using the assigned translator object.
        Args:
            input_data (Union[str, List[str]]): The input data to be translated. It can be either a string or a list of strings.
            src (str): The source language code.
            dest (str): The destination language code.
            fail_translation_code (str, optional): The code to be returned in case of translation failure. Defaults to "P1OP1_F".
        Returns:
            Union[str, List[str]]: The translated output data. It will have the same type as the input data.
        Raises:
            TypeError: If the input_data is not of type str or List[str], or if the elements of input_data list are not of type str.
        Notes:
            - The translator object instance must be assigned to self.translator before calling this method.
            - The translation is performed by calling the _do_translate() method.
        """

        global GLOBAL_CACHE_FAIL_PROMPT

        # Type check for input_data
        if not isinstance(input_data, (str, list)):
            raise TypeError(f"input_data must be of type str or List[str], not {type(input_data).__name__}")

        if isinstance(input_data, list) and not all(isinstance(item, str) for item in input_data):
            raise TypeError("All elements of input_data list must be of type str")

        # Ensure the translator is set
        assert self.translator, "Please assign the translator object instance to self.translator"

        if len(GLOBAL_CACHE_FAIL_PROMPT) > 5000:
            _, GLOBAL_CACHE_FAIL_PROMPT = pop_half_dict(GLOBAL_CACHE_FAIL_PROMPT)
        
        parametrized_hash = hash_input(self.__get_hashable_key(input_data, src, dest, fail_translation_code))

        try:
            # Perform the translation
            translated_instance = self._do_translate(input_data,
                                                    src=src,
                                                    dest=dest,
                                                    fail_translation_code=fail_translation_code)
            if parametrized_hash in GLOBAL_CACHE_FAIL_PROMPT:
                GLOBAL_CACHE_FAIL_PROMPT.pop(parametrized_hash) 
        except Exception as e:
            # Check if the exception is unavoidable by matching the prompt with the cache fail prompt key
            if parametrized_hash in GLOBAL_CACHE_FAIL_PROMPT:
                if isinstance(input_data, list) and GLOBAL_CACHE_FAIL_PROMPT[parametrized_hash] >= GLOBAL_MAX_LIST_RETRIES:
                    logger.warning("Unavoidable exception: %s\nGlobal max retries reached for list translation", e)
                    return [fail_translation_code, fail_translation_code]
                elif isinstance(input_data, str) and GLOBAL_CACHE_FAIL_PROMPT[parametrized_hash] >= GLOBAL_MAX_STRING_RETRIES:
                    logger.warning("Unavoidable exception: %s\nGlobal max retries reached for string translation", e)
                    return fail_translation_code
                else:
                    GLOBAL_CACHE_FAIL_PROMPT[parametrized_hash] += 1
            else:
                GLOBAL_CACHE_FAIL_PROMPT[parametrized_hash] = 1

            logger.debug("Current global fail cache: %s", GLOBAL_CACHE_FAIL_PROMPT)
            raise e


        assert type(input_data) == type(translated_instance),\
            f" The function self._do_translate() return mismatch datatype from the input_data," \
            f" expected {type(input_data)} from self._do_translate() but got {type(translated_instance)}"

        return translated_instance


