'''
Regrouprement des fonctions pour automatiser le pretraitement

Utilisation:
mon_fichier = 'donnee/fr.openfoodfacts.org.products.csv'    
pretraitement_donnee(mon_fichier)
On obtient un fichier en sortie:
'donnee/fr.openfoodfacts.org.products_pretraitement.csv'
'''
import numpy as np
import pandas as pd
import random
import os


mon_fichier = 'donnee/fr.openfoodfacts.org.products.csv'

def lecture_fichier(mon_fichier, echantillon=True):
    '''
    Lecture du fichier de donnee csv, retourne un DataFrame data
    
    mon_fichier: nom du fichier
    echantillon=False : lecture de tout le fichier
    echantillon=True : lecture d´un echantillon de 10000 lignes
    '''
    
    ## on impose un dtype aux colonnes qui ont un dtype mélangé
    ## cela evite les warnings a la lecture
    liste_colonne_dtypes_object = ['code','abbreviated_product_name', 'packaging_text', 
                               'manufacturing_places','manufacturing_places_tags', 'emb_codes', 
                               'emb_codes_tags','first_packaging_code_geo', 'cities_tags',
                               'additives','ingredients_from_palm_oil_tags', 
                               'ingredients_that_may_be_from_palm_oil_tags',
                               'brand_owner'] 
    liste_dtypes = []
    for colonne in liste_colonne_dtypes_object:
        liste_dtypes.append((colonne,'string'))
    liste_dtypes.append(('energy-kj_100g','float64'))
    liste_dtypes.append(('energy_100g','float64'))
  
    dict_dtypes = dict(liste_dtypes)
    
    ## lecture sur un echantillon du fichier
    if echantillon: 
        
        ## Calcul du nombre de ligne du fichier : 1,8 Millions de lignes
        with open(mon_fichier, 'r', encoding="utf8") as file:
            nombre_ligne_fichier = 0
            for line in file:
                nombre_ligne_fichier += 1
                
        ## On tire au sort sans remise les lignes que l´on va lire
        n_lignes = 10000
        ligne_lecture = random.sample(range(1,nombre_ligne_fichier),n_lignes)
        ligne_lecture.append(0) ## on lit la premiere ligne dans tous les cas
        ligne_lecture.sort()
        ligne_lecture_exclue = list(set(range(nombre_ligne_fichier))-set(ligne_lecture))

        ## Lecture du fichier en excluant toutes les lignes sauf celles tirees au sort
        data = pd.read_csv( mon_fichier,sep='\t',
                           encoding='utf_8', 
                           skiprows = ligne_lecture_exclue,
                           dtype=dict_dtypes ) 
    ## lecture complete
    else:    
        data = pd.read_csv( mon_fichier,sep='\t',
                           encoding='utf_8', 
                           dtype=dict_dtypes )
    return data

def ecriture_fichier(data,mon_fichier):
    '''
    Enregistre les resultats au format csv,
    le nom de fichier finit par "_pretraitement.csv"
    ''' 
    fichier, fichier_extension = os.path.splitext(mon_fichier)
    fichier_sauvegarde = fichier + '_pretraitement' + fichier_extension
    data.to_csv(fichier_sauvegarde)

def retire_colonne(data):
    '''Enleve les colonnes vide a 99 % du dataframe data
    et retourne le dataframe'''
    
    ## nombre de colonnes remplies au minimum a 1%
    sous_ensemble = data.columns[data.count()>=(1/100)*data.shape[0]].to_list()
    sous_ensemble.append('fruits-vegetables-nuts-estimate_100g')
    return data[sous_ensemble].copy()

def decoupage(data):
    """decoupage en categorie"""
    
    data_general_information = data.loc[:,:'quantity']
    data_tags = data.loc[:,'packaging':'countries_fr']
    data_ingredients = data.loc[:,'ingredients_text':'traces_fr']
    data_misc = data.loc[:,'serving_size':'image_nutrition_small_url']
    data_nutrition_facts = data.loc[:,'energy-kj_100g':]

    ## On verifie que l´on a pas oublié de colonnes dans le decoupage en categories
    data_set_decoupage = set(data_general_information.columns.to_list()).union(data_nutrition_facts.columns,
                                                         data_misc.columns,
                                                         data_ingredients.columns,
                                                         data_tags.columns,
                                                         data_general_information.columns)
    data_set_original = set(data.columns)
    data_set_difference = data_set_original.difference(data_set_decoupage)
    if data_set_difference != set():
        print("Il manque {} après le decoupage: ".format(data_set_difference))
            
    return [data_general_information,data_tags,data_ingredients,data_misc,data_nutrition_facts] 

def selection_colonnes(data_general_information,data_tags,data_ingredients,data_misc,data_nutrition_facts):

    ## Champs General information
    ## data_general_information.columns.to_list()
    mes_champs = ['code',
                  'url',
                  'creator',
                  'created_t',
                  'created_datetime',
                  'last_modified_t',
                  'last_modified_datetime',
                  'product_name',
                  'quantity' ]
    data_general_information = data_general_information[mes_champs].copy()

    ## Champs Tags
    ## data_tags.columns.to_list()
    mes_champs = ['packaging',
                  'brands',
                  'categories_fr',
                  'origins_fr',
                  'manufacturing_places',
                  'labels_fr',
                  'purchase_places',
                  'stores',
                  'countries_fr']
    data_tags = data_tags[mes_champs].copy()
    
    ## Champs Ingredients
    ## data_ingredients.columns.to_list()
    mes_champs = ['ingredients_text',
                  'allergens',
                  'traces_fr']
    data_ingredients = data_ingredients[mes_champs].copy()
    
    ## Champs Misc
    ## data_misc.columns.to_list()
    mes_champs = [
     'additives_n',
     'additives_fr',
     'nutriscore_score',
     'nutriscore_grade',
     'nova_group',
     'pnns_groups_1',
     'pnns_groups_2',
     'brand_owner',
     'ecoscore_score_fr',
     'ecoscore_grade_fr',
     'main_category_fr',
    ]
    data_misc = data_misc[mes_champs].copy()
    
    ## Champs Nutrition facts
    data_nutrition_facts.columns.to_list()
    mes_champs = [
        'energy-kcal_100g',
        'energy_100g',
        'fat_100g',
        'saturated-fat_100g',
        'carbohydrates_100g',
        'sugars_100g',
        'fiber_100g',
        'proteins_100g',
        'salt_100g',
        'sodium_100g',
        'nutrition-score-fr_100g'
    ]
    data_nutrition_facts = data_nutrition_facts[mes_champs].copy()
    
    return [data_general_information,data_tags,data_ingredients,data_misc,data_nutrition_facts]

def selection_ligne_nutrition(data):
    ## on selectionne les lignes avec l´energie renseignée
    masque_info_nutrition = data["energy_100g"].notna() 
    masque_ingredient = True ##data_ingredients["ingredients_text"].notna(), pas pris en compte !
    masque_produits = masque_ingredient & masque_info_nutrition
    return data[masque_produits].copy()

def selection_ligne_categorie(data):
    masque_categorie = ~data['pnns_groups_1'].isin(['unknown']) ## valeurs renseignées et connues
    return data[masque_categorie].copy()

def change_nom_categorie(categorie_old,categorie_new,data):
    masque = data["pnns_groups_1"].isin([categorie_old])
    data.loc[masque,"pnns_groups_1"]= categorie_new
    return data.copy()

def change_nom_categorie_pnns2(data):
    # on regroupe les categories qui ont le meme nom. La seule difference est l´ecriture en minuscules ald capitalized 
    categorie_set = set(data["pnns_groups_2"].unique()).intersection(set(data["pnns_groups_2"].str.lower().unique()))
    print(categorie_set)
    categorie_set.remove('unknown')
    for nom in categorie_set:
        masque = data["pnns_groups_2"].isin([nom])
        data.loc[masque,"pnns_groups_2"]= nom.capitalize()
        return data.copy()

def energie(data):
    """on veut les valeurs dans un intervalle [min,max]"""
    seuil_min_energie = 10
    seuil_max_energie = 3800
    masque_energie = ( data["energy_100g"] < seuil_max_energie ) & ( data["energy_100g"] > seuil_min_energie )
    return data[masque_energie].copy()

def poids_masque(data, min,max):
    """calcul du masque pour avoir les valeurs dans l´intervalle souhaité"""
    masque = ( data > min ) & ( data < max )
    return masque

def poids(data):
    """On encadre le poids total et le poids de chaque nutriments"""
    seuil_min_poids = -1
    seuil_max_poids = 101
    
    nutriments_liste = ['fat_100g','carbohydrates_100g','proteins_100g','salt_100g']
    nutriments_liste_sous_groupe= ['saturated-fat_100g','sugars_100g','fiber_100g']
    
    ## sous groupe
    for nutriment in nutriments_liste_sous_groupe:
        masque = poids_masque(data[nutriment],-1,101)
        data.loc[~masque,nutriment]=np.nan

    ## groupe
    masque_nutriments = poids_masque(data['fat_100g'],-1,101)
    for ind_nutriments in nutriments_liste:
        masque_nutriments &= poids_masque(data[ind_nutriments],-1,101) == True
        
    ## poids total
    data_somme=0
    for ind_nutriments in nutriments_liste:
        data_somme += data[ind_nutriments]
    masque_nutriments_somme = poids_masque(data_somme,-1,101)

    masque_poids = masque_nutriments_somme & masque_nutriments
    
    return data[masque_poids].copy()

def pretraitement_donnee(mon_fichier,enregistrement=False): 
    """
    Fonction de pretraitement global des données
    
    En entree, le nom du fichier.csv,
    enregistrement: sauvegarde le resultat dans un autre fichier fichier_pretraitement.csv
    
    En sortie le jeu de donnees pretraité
    
    data = pretraitement('fichier.csv') 
    ou
    data = pretraitement('fichier.csv',enregistrement=True)
    """
    
    ## lecture du fichiers
    print('Lecture du fichier')
    data = lecture_fichier(mon_fichier,echantillon=False)
    
    ## selection des colonnes
    print('Selection des colonnes')
    data = retire_colonne(data)
    
    [data_general_information,data_tags,data_ingredients,data_misc,data_nutrition_facts] = decoupage(data)
    
    [data_general_information,
     data_tags,
     data_ingredients,
     data_misc,
     data_nutrition_facts] = selection_colonnes(data_general_information,
                                                  data_tags,
                                                  data_ingredients,
                                                  data_misc,
                                                  data_nutrition_facts)
    
    data = pd.concat([data_general_information,data_tags,data_ingredients,data_misc,data_nutrition_facts],axis=1)
    
    ## selection des lignes
    print('Selection des lignes')
    data = selection_ligne_nutrition(data)
   
    data = change_nom_categorie_pnns2(data)
    categorie_old = 'fruits-and-vegetables'
    categorie_new = 'Fruits and vegetables'
    data = change_nom_categorie(categorie_old,categorie_new,data)

    categorie_old = 'sugary-snacks'
    categorie_new = 'Sugary snacks'
    data = change_nom_categorie(categorie_old,categorie_new,data)

    categorie_old = 'cereals-and-potatoes'
    categorie_new = 'Cereals and potatoes'
    data = change_nom_categorie(categorie_old,categorie_new,data)

    categorie_old = 'salty-snacks'
    categorie_new = 'Salty snacks'
    data = change_nom_categorie(categorie_old,categorie_new,data)
    
    ## valeurs renseignées et connues
    data = selection_ligne_categorie(data)
    
    ## traitement des valeurs aberrantes
    print('Traitement des valeurs aberrantes')
    data = energie(data)
    data = poids(data)
    
    if enregistrement:
        ## ecriture du fichier
        print('Ecriture du fichier')
        ecriture_fichier(data,mon_fichier)
    
    return data
    

mon_fichier = 'donnee/fr.openfoodfacts.org.products.csv'    
data = pretraitement_donnee(mon_fichier,enregistrement=True)
