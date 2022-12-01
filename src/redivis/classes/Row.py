import copy
import operator
from typing import Any, Dict, Iterable, Tuple


class Row(object):
    """Represents a row in a table or query result.
    Values can be accessed by position (index), by key like a dict,
    or as properties.
    Args:
        values (Sequence[object]): The row values
        variable_name_to_index (Dict[str, int]):
            A mapping from variable names to indexes
    """

    # Avoid conflict with schema fields.
    __slots__ = ("__redivis_hidden_values", "__redivis_hidden_variable_name_to_index")

    def __init__(self, values, variable_name_to_index) -> None:
        self.__redivis_hidden_values = values
        self.__redivis_hidden_variable_name_to_index = variable_name_to_index

    def values(self):
        """Return the values included in this row.
        Returns:
            Sequence[object]: A sequence of length ``len(row)``.
        """
        return copy.deepcopy(self.__redivis_hidden_values)

    def keys(self) -> Iterable[str]:
        """Return the keys for using a row as a dict.
        Returns:
            Iterable[str]: The keys corresponding to the columns of a row
        """
        return self.__redivis_hidden_variable_name_to_index.keys()

    def items(self) -> Iterable[Tuple[str, Any]]:
        """Return items as ``(key, value)`` pairs.
        Returns:
            Iterable[Tuple[str, object]]:
                The ``(key, value)`` pairs representing this row.
        """
        for key, index in self.__redivis_hidden_variable_name_to_index.items():
            yield key, self.__redivis_hidden_values[index]

    def get(self, key: str) -> Any:
        """Return a value for key, with a default value if it does not exist.
        Args:
            key (str): The key of the column to access
        Returns:
            object:
                The value associated with the provided key
        """
        index = self.__redivis_hidden_variable_name_to_index.get(key)
        if index is None:
            raise KeyError(f"Row does not contain field {key}")
        return self.__redivis_hidden_values[index]

    def __getattr__(self, name):
        value = self.__redivis_hidden_variable_name_to_index.get(name)
        if value is None:
            raise AttributeError(f"Row does not contain field {name}")
        return self.__redivis_hidden_values[value]

    def __len__(self):
        return len(self.__redivis_hidden_values)

    def __getitem__(self, key):
        if isinstance(key, str):
            value = self.__redivis_hidden_variable_name_to_index.get(key)
            if value is None:
                raise KeyError(f"Row does not contain field {key}")
            key = value
        return self.__redivis_hidden_values[key]

    def __eq__(self, other):
        if not isinstance(other, Row):
            return NotImplemented
        return (
            self.__redivis_hidden_values == other.__redivis_hidden_values
            and self.__redivis_hidden_variable_name_to_index == other.__redivis_hidden_variable_name_to_index
        )

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        column_index_to_variable_name = {}
        for variable_name in self.__redivis_hidden_variable_name_to_index.keys():
            column_index_to_variable_name[self.__redivis_hidden_variable_name_to_index[variable_name]]=variable_name
        cells = []
        for index, value in enumerate(self.__redivis_hidden_values):
            cells.append(f"{column_index_to_variable_name[index]}: {value}")

        f2i = "{" + ", ".join(cells) + "}"
        return f"Row({f2i})"