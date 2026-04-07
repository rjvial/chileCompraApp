


def remove_accents(input_str):
    try:
        accent_map_lower = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'à': 'a', 'è': 'e', 'ì': 'i', 'ò': 'o', 'ù': 'u',
            'ä': 'a', 'ë': 'e', 'ï': 'i', 'ö': 'o', 'ü': 'u',
            'â': 'a', 'ê': 'e', 'î': 'i', 'ô': 'o', 'û': 'u',
            'ã': 'a', 'õ': 'o',
            'ñ': 'n',
            'ç': 'c',
            'ø': 'o',
            'œ': 'oe'
        }
        input_out = input_str.lower()
        input_out = input_out.translate(str.maketrans(accent_map_lower))
    except:
        input_out = input_str
    return input_out


def remove_accents_and_spaces(input_str):
    accent_map_lower = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'à': 'a', 'è': 'e', 'ì': 'i', 'ò': 'o', 'ù': 'u',
        'ä': 'a', 'ë': 'e', 'ï': 'i', 'ö': 'o', 'ü': 'u',
        'â': 'a', 'ê': 'e', 'î': 'i', 'ô': 'o', 'û': 'u',
        'ã': 'a', 'õ': 'o',
        'ñ': 'n',
        'ç': 'c',
        'ø': 'o',
        'œ': 'oe'
    }
    input_out = input_str.lower()
    input_out = input_out.translate(str.maketrans(accent_map_lower))
    input_out = input_out.replace(' ', '_')
    return input_out
