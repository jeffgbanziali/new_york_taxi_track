from database import execute_query

def get_zones_data(borough, limit):
    query = "SELECT borough, zone, nb_trajets, ca_total, tarif_moyen, pourboire_moyen, distance_moy, duree_moy, rang_borough FROM kpi_par_zone"
    params = []
    
    if borough and borough != "Tous":
        query += " WHERE borough = %s"
        params.append(borough)
        
    query += " ORDER BY ca_total DESC"
    
    if limit:
        query += " LIMIT %s"
        params.append(limit)
        
    return execute_query(query, tuple(params))

def get_heures_data():
    query = "SELECT heure, jour_semaine, mois, nb_trajets, ca_total, tarif_moyen FROM kpi_par_heure ORDER BY heure ASC"
    return execute_query(query)

def get_paiements_data(borough):
    query = "SELECT borough, mode_paiement, nb_trajets, tarif_moyen FROM kpi_paiement"
    params = []
    if borough and borough != "Tous":
        query += " WHERE borough = %s"
        params.append(borough)
        
    return execute_query(query, tuple(params))

def get_aeroports_data():
    query = "SELECT aeroport, nb_trajets, tarif_moyen, distance_moy, duree_moy FROM kpi_aeroports ORDER BY nb_trajets DESC"
    return execute_query(query)

def get_meteo_data():
    query = "SELECT meteo, temp_moy, pluie_moy, nb_trajets, tarif_moyen, pourboire_moyen, duree_moy FROM kpi_meteo ORDER BY nb_trajets DESC"
    return execute_query(query)