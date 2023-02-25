import unicodedata
import re

def remove_accents(string):
    normalize_string = unicodedata.normalize('NFD', string)
    caracters_without_accents = [caracter for caracter in normalize_string if unicodedata.category(caracter) != 'Mn']
    return ''.join(caracters_without_accents)

def extract_alphanumeric(value):
    return re.sub('\W+','', str(value))