root_catalog = {
    "id": "dis_2_1_winbaarheidgrids",
    "title": 'DIS 2.1 Winbaarheid grids',
    "description": ("This collection comprises spatial and temporal data assets related to the management of sand supplies in the North Sea, "
                "with an emphasis on the distribution, quality, and extractability of sand layers. Developed by Deltares/TNO under the "
                "commission of the Sea and Delta Service of Rijkswaterstaat, the mineral information system (DIS) at the core of this collection "
                "enables the rapid evaluation of extractable sand quantities across different depths (up to 12 meters) tailored to various sand quality "
                "requirements. While the system facilitates strategic decision-making on sand extraction at a regional scale, it is not designed for "
                "detailed lithological sediment analysis within potential extraction zones. The collection also underscores the importance of continuous "
                "updates with new research findings, including significant insights from the 2016 large-scale exploration of new sand extraction areas "
                "(Blauw et al., 2017), ensuring sustainable and effective sand management for coastline care and commercial uses amidst increasing maritime activity."),
    "keywords": ["North Sea", "Sand Management", "Sand Quality", "Mineral Information System", "Sand Extraction", "Coastline Care", "Deltares", "TNO", "Rijkswaterstaat"],
    "license": "Appropriate License",
    "extent": {
        "spatial_coverage": "North Sea Region",
        "temporal_coverage_start": "Data Collection Start Date",
        "temporal_coverage_end": "Present"
    },
    "providers": [
        {
            "name": "Deltares/TNO",
            "roles": ["Provider", "Creator"],
            'url': 'https://www.deltares.nl/expertise/projecten/zand-uit-de-noordzee/'
        }
        ,
        {
            "name": "TNO",
            "roles": ["Provider", "Creator"],
            "url": "https://www.dinoloket.nl/dis"
        },
        {
            "name": "Rijkswaterstaat",
            "roles": ["Commissioner"],
            "url": "https://maps.rijkswaterstaat.nl/gwproj55/index.html?viewer=ZD_Zandwinstrategie.Webviewer",
        }  
    ],  
}


# Scenario info to be appended, ids need to be unique. For different scenarios we use the root id + _scenario1 or _scenario2 or _scenario3 or _scenario4 or _scenario999

scenario1_catalog = {
    'id': "scenario1",
    "title": "Scenario 1",
    "description": ("Scenario 1 of the DIS 2.1 Winbaarheid grids."),
}


scenario2_catalog = {
    'id': "scenario2",
    "title": "Scenario 2",
    "description": ("Scenario 2 of the DIS 2.1 Winbaarheid grids."),
}

scenario3_catalog = {
    'id': "scenario3",
    "title": "Scenario 3",
    "description": ("Scenario 3 of the DIS 2.1 Winbaarheid grids."),
}

scenario4_catalog = {
    'id': "scenario4",
    "title": "Scenario 4",
    "description": ("Scenario 4 of the DIS 2.1 Winbaarheid grids."),
}

scenario999_catalog = {
    'id': "scenario999_alleenveen",
    "title": "Scenario 999 Aleenveen",
    "description": ("Scenario 999 Aleenveen of the DIS 2.1 Winbaarheid grids."),
}
   
# Zondergeo Metgeo info to be appended, ids need to be unique. For different scenarios we use the original id + _metgeo or _zondergeo

metgeo_catalog = {
    'id': "metgeo",
    "title": "Metgeo",
    "description": "with geology",
}
    
zondergeo_catalog = {
    'id': "zondergeo",
    "title": "Zondergeo",
    "description": "without geology",
}

stoor_geen_collection = {
    'id': "stoor_geen",
    "title": "Stoor geen",
    "description": "Stoor geen",
}

# Stoor info to be appended, ids need to be unique. For different scenarios we use the original id + _stoor0_5m or _stoor1m or _stoor2m

stoor0_5m_collection = {
    'id': "stoor0_5m",
    "title": "Stoor 0.5m",
    "description": "Stoor 0.5m",   
}
stoor1m_collection = {
    'id': "stoor1m",
    "title": "Stoor 1m",
    "description": "Stoor 1m",      
}

stoor2m_collection = {
    'id': "stoor2m",
    "title": "Stoor 2m",
    "description": "Stoor 2m",      
}

dict_names={
    "scenario1": scenario1_catalog,
    "scenario2": scenario2_catalog,	
    "scenario3": scenario3_catalog,
    "scenario4": scenario4_catalog,
    "scenario999_alleenveen": scenario999_catalog,
    "metgeo": metgeo_catalog,
    "zondergeo": zondergeo_catalog,
    "stoor_geen": stoor_geen_collection,
    "stoor0_5m": stoor0_5m_collection,
    "stoor1m": stoor1m_collection,
    "stoor2m": stoor2m_collection,
    "root": root_catalog
} 
