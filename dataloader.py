import json
import pandas as pd

def process_discovery(discovery):
        new_discovery = {}
        new_discovery['id'] = discovery['_id']['$oid']
        new_discovery['prompt'] = discovery['prompt']
        new_discovery['view_count'] = discovery['view_count']
        new_discovery['discoveries'] = []
        new_discovery['language'] = discovery['language']
        new_destinations = []
        for destination in discovery['discoveries']:
            try:
                new_destinations.append({
                    'parent_id': new_discovery['id'],
                    'name': destination['name'],
                    'description': destination['description'],
                    'countrycode': destination['countrycode'],
                    'country': destination['country'],
                    'photo': destination['photo'],
                    'latitude': destination['latitude'],
                    'longitude': destination['longitude'],
                    'language': discovery['language']})
            except Exception as e:
                continue
            
        return new_discovery,new_destinations


class VooyaiDataloader:

    def __init__(self,file_path):
        with open(file_path+"vooyai.discoveries.json") as file:
            #Load discoveries
            raw_discoveries = json.load(file)

            # Get clean discoveries and destinations
            self.discoveries = []
            self.destinations = []
            for raw_discovery in raw_discoveries:
                clean_discovery, clean_destinations = process_discovery(raw_discovery)
                self.discoveries.append(clean_discovery)
                self.destinations.extend(clean_destinations)

            print(len(self.destinations))
            #Now create (and clean) dataframes
            self.discoveries_df = pd.DataFrame(self.discoveries)
            self.destinations_df = pd.DataFrame(self.destinations)
           

        
        with open(file_path+"vooyai.itineraries.json") as file:
            self.itineraries = json.load(file)

        with open(file_path+"vooyai.metrics.json") as file:
            self.metrics = json.load(file)

        with open(file_path+"vooyai.sites.json") as file:
            self.sites = json.load(file)

    



