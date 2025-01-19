import tinytuya
import mysql.connector
import time

# Verbindung zur MariaDB herstellen
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host="192.168.10.100",  
            user="twitter",   # Benutzername
            password="root", # Passwort
            database="home-dashboard" # Datenbankname
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Fehler: {err}")
        return None
    
def insert_regler_data(sensor_name, t1, t2, t3):
    connection = connect_to_db()
    if connection is None:
        return

    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO tuya_regler (sensorName, T1, T2, T3)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (sensor_name, t1, t2, t3))
        connection.commit()
        print(f"Daten für {sensor_name} erfolgreich eingefügt.")
    except mysql.connector.Error as err:
        print(f"Fehler beim Einfügen der Daten: {err}")
    finally:
        cursor.close()
        connection.close()
        
def insert_zaehler_data(sensor_name, zaehlerstand):
    connection = connect_to_db()
    if connection is None:
        return

    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO tuya_zaehler (sensorName, Zaehlerstand)
            VALUES (%s, %s)
        """
        cursor.execute(insert_query, (sensor_name, zaehlerstand))
        connection.commit()
        print(f"Daten für {sensor_name} erfolgreich eingefügt.")
    except mysql.connector.Error as err:
        print(f"Fehler beim Einfügen der Daten: {err}")
    finally:
        cursor.close()
        connection.close()

# Daten aus dataB und dataC extrahieren und speichern
def save_regler_to_db(data, sensor_name):
    try:
        t1 = data['dps'].get('26', None)  # T1 auslesen
        t2 = data['dps'].get('22', None)  # T2 auslesen
        t3 = data['dps'].get('21', None)  # T3 auslesen
        insert_regler_data(sensor_name, t1, t2, t3)
    except KeyError as e:
        print(f"Fehler beim Extrahieren der Daten: {e}")

def save_zaehler_to_db(data, sensor_name):
    try:
        zaehlerstand = data['dps'].get('1', None)  # Zaehlerstand auslesen
        insert_zaehler_data(sensor_name, zaehlerstand)
    except KeyError as e:
        print(f"Fehler beim Extrahieren der Daten: {e}")


def get_regler_daten(run_count):
    a = tinytuya.Device('bf21619f5c6537ad93zz3j', '192.168.10.26', "!/xV$j{-DXm=v?'p", version=3.4)
    b = tinytuya.Device('bf4cef29a4753deb66xnra', '192.168.10.25', "[^/;E`VHcSIGf+fI", version=3.3)
    c = tinytuya.Device('bf884e4b13be13f8c4bdxx', '192.168.10.24', "e$OehoaU!;p8O0u6", version=3.3)

    dataB = b.status()
    dataC = c.status()
    save_regler_to_db(dataB, "Leda Kachelofen - Regler")
    save_regler_to_db(dataC, "Roth Solaranlage - Regler")

    # Hauptschalter-Stromzähler nur alle 60 Durchläufe abfragen
    if run_count % 1440 == 0:
        dataA = a.status()
        save_zaehler_to_db(dataA, "Hauptschalter - Stromzähler")

def run_scheduler():
    run_count = 0
    while True:
        run_count += 1
        get_regler_daten(run_count)
        time.sleep(60)

if __name__ == "__main__":
    run_scheduler()
