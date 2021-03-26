# import libraries
import openrouteservice
import folium

# Specify your personal API key
client = openrouteservice.Client(
    key='5b3ce3597851110001cf6248171a4636f30449aa9e446f0682f74ebb')

# this is declared locally. This is the structure of the centroids to calculate isochrones
# dict = {
#     'first': { #office
#         'location': [11.2607037, 44.4841799] #swapped lat,long -> long, lat
#     },
#     'second': { #due torri
#         'location': [11.3445425, 44.4942094]
#     }
# }


def plot_isochrones(lat, long, interval_sec, dict_, zm=12):

    # Set up folium map
    map1 = folium.Map(
        tiles='Stamen Toner',  # visual
        location=([long, lat]),  # visual start at
        zoom_start=zm)  # initial zoom at start

    # Request of isochrones with 25 minute bike ride.
    params_iso = {
        'profile': 'cycling-regular',
        'intervals': [interval_sec],  # 1800/60 = 30 mins
        'segments': interval_sec
    }

    for name, loc in dict_.items():
        # Add apartment coords to request parameters
        params_iso['locations'] = [loc['location']]
        loc['iso'] = client.isochrones(
            **params_iso)  # Perform isochrone request
        folium.features.GeoJson(loc['iso']).add_to(map1)  # Add GeoJson to map

        folium.map.Marker(list(reversed(loc['location'])),  # reverse coords due to weird folium lat/lon syntax
                          icon=folium.Icon(color='lightgray',
                                           icon_color='#cc0000',
                                           icon='map-marker',
                                           prefix='fa',
                                           ),
                          popup=name,
                          ).add_to(map1)  # Add isochrones to map

    # show map
    return map1
