"""
Mapping of Brazilian CEP regions to approximate coordinates.
Based on the real structure of Brazilian CEPs (ZIP codes).
"""
# Mapeamento de prefixos de CEP para estados e coordenadas aproximadas
CEP_REGIONS = {
    # São Paulo (01000-19999)
    1: {
        'state': 'SP',
        'cities': [
            ('sao paulo', -23.5505, -46.6333),
            ('campinas', -22.9099, -47.0626),
            ('santos', -23.9608, -46.3333),
        ]
    },
    # Rio de Janeiro (20000-28999)
    2: {
        'state': 'RJ',
        'cities': [
            ('rio de janeiro', -22.9068, -43.1729),
            ('niteroi', -22.8839, -43.1039),
            ('petropolis', -22.5051, -43.1787),
        ]
    },
    # Minas Gerais (30000-39999)
    3: {
        'state': 'MG',
        'cities': [
            ('belo horizonte', -19.9167, -43.9345),
            ('uberlandia', -18.9188, -48.2768),
            ('juiz de fora', -21.7642, -43.3502),
        ]
    },
    # Bahia (40000-48999)
    4: {
        'state': 'BA',
        'cities': [
            ('salvador', -12.9714, -38.5014),
            ('feira de santana', -12.2664, -38.9663),
            ('vitoria da conquista', -14.8615, -40.8442),
        ]
    },
    # Pernambuco (50000-56999)
    5: {
        'state': 'PE',
        'cities': [
            ('recife', -8.0476, -34.8770),
            ('olinda', -8.0089, -34.8553),
            ('caruaru', -8.2837, -35.9761),
        ]
    },
    # Ceará (60000-63999)
    6: {
        'state': 'CE',
        'cities': [
            ('fortaleza', -3.7172, -38.5433),
            ('caucaia', -3.7361, -38.6531),
            ('juazeiro do norte', -7.2131, -39.3151),
        ]
    },
    # Distrito Federal (70000-72799, 73000-73999)
    7: {
        'state': 'DF',
        'cities': [
            ('brasilia', -15.7939, -47.8828),
        ]
    },
    # Paraná (80000-87999)
    8: {
        'state': 'PR',
        'cities': [
            ('curitiba', -25.4284, -49.2733),
            ('londrina', -23.3045, -51.1696),
            ('maringa', -23.4205, -51.9331),
        ]
    },
    # Rio Grande do Sul (90000-99999)
    9: {
        'state': 'RS',
        'cities': [
            ('porto alegre', -30.0346, -51.2177),
            ('caxias do sul', -29.1634, -51.1797),
            ('pelotas', -31.7654, -52.3376),
        ]
    },
}
# Estados adicionais para CEPs especiais
ADDITIONAL_REGIONS = {
    # Sergipe (49000-49999)
    49: {
        'state': 'SE',
        'cities': [
            ('aracaju', -10.9472, -37.0731),
        ]
    },
    # Alagoas (57000-57999)
    57: {
        'state': 'AL',
        'cities': [
            ('maceio', -9.6658, -35.7350),
        ]
    },
    # Paraíba (58000-58999)
    58: {
        'state': 'PB',
        'cities': [
            ('joao pessoa', -7.1195, -34.8450),
        ]
    },
    # Rio Grande do Norte (59000-59999)
    59: {
        'state': 'RN',
        'cities': [
            ('natal', -5.7945, -35.2110),
        ]
    },
    # Pará (66000-68899)
    66: {
        'state': 'PA',
        'cities': [
            ('belem', -1.4558, -48.5039),
        ]
    },
    # Amazonas (69000-69299, 69400-69899)
    69: {
        'state': 'AM',
        'cities': [
            ('manaus', -3.1190, -60.0217),
        ]
    },
    # Santa Catarina (88000-89999)
    88: {
        'state': 'SC',
        'cities': [
            ('florianopolis', -27.5954, -48.5480),
            ('joinville', -26.3045, -48.8487),
            ('blumenau', -26.9194, -49.0661),
        ]
    },
    # Goiás (72800-72999, 73700-76799)
    73: {
        'state': 'GO',
        'cities': [
            ('goiania', -16.6869, -49.2648),
            ('aparecida de goiania', -16.8173, -49.2437),
        ]
    },
}

def get_region_info(cep_prefix: int):
    """
    Returns region information based on CEP prefix.
    
    Args:
        cep_prefix: CEP prefix (first 5 digits)
    
    Returns:
        Tuple (state, city, lat, lng)
    """
    import random
    
    # Try to find region by first digit
    first_digit = cep_prefix // 10000
    
    if first_digit in CEP_REGIONS:
        region = CEP_REGIONS[first_digit]
    else:
        # Try to find region by the first two digits (for special cases)
        two_digits = cep_prefix // 1000
        if two_digits in ADDITIONAL_REGIONS:
            region = ADDITIONAL_REGIONS[two_digits]
        else:
            # Fallback: use São Paulo as default
            region = CEP_REGIONS[1]
    
    # Pick a random city from the region and add some random noise to coordinates
    city, lat, lng = random.choice(region['cities'])
    state = region['state']
    
    # Add random variation (±0.1 degrees ≈ 11km)
    lat += random.uniform(-0.1, 0.1)
    lng += random.uniform(-0.1, 0.1)
    
    return state, city, round(lat, 6), round(lng, 6)

def validate_cep(cep):
    """
    Validates if a ZIP code is in the correct format.
    
    Args:
        cep: ZIP code as int or string
    
    Returns:
        Validated ZIP code as int or None
    """
    try:
        if isinstance(cep, str):
            # Remove hyphens and spaces
            cep = cep.replace('-', '').replace(' ', '')
            cep = int(cep)
        
        # ZIP code must have 5 digits (prefix) or 8 digits (complete)
        if 1000 <= cep <= 99999:  # Prefix
            return cep
        elif 10000000 <= cep <= 99999999:  # Complete
            return cep // 1000  # Return only the prefix
        else:
            return None
    except:
        return None