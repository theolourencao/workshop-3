from typing import Union, Dict

GenericSchema = Dict[str, Union[str, float, int]]

CompraSchema : GenericSchema = {
"ean": str,
"price" : float,
"store" : int,
"datetime": str
}