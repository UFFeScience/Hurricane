import pandas as pd
import numpy as np
from datetime import datetime
from hurricane.utils.datalake import Datalake
from hurricane.data.api.heuristic import Heuristic as APIHeuristic

import pandas as pd

class Heuristic:
    def process_generate_heuristic_files(**args):
        dag_id   = args['dag_id']
        adl      = args['adl']
        datalake = Datalake(adl, dag_id)
        api      = APIHeuristic(datalake, dag_id)

        date = datetime.today().strftime('%Y%m%d%H%M%S')

        selected_zones = [1, 3]
        number_police = 2
        number_inter_zone_routes = 0
        number_intra_zone_routes = 3
        max_distance_inter_zone  = 5000.0
        max_distance_intra_zone  = 1500.0
        vip_routes = [
        #    "1 3 112622	112322 112590 112282 112889	112241",
        #    "2 3 82007 82512 88001 88009 88573 88351"
        ]
        filename = f"/home/maiconbanni/Documentos/UFF/datalake/output/{date}.txt"
        api.get_infos(  selected_zones,
                        number_police,
                        number_inter_zone_routes,
                        number_intra_zone_routes,
                        max_distance_inter_zone,
                        max_distance_intra_zone,
                        vip_routes,
                        filename)


