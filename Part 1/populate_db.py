import pyodbc
from csv import reader

TABLES = [
    ("data/date.csv", "Date"),
    ("data/tourney.csv", "Tournament"),
    ("data/geography.csv", "Geography"),
    ("data/player.csv", "Player"),
    ("data/match.csv", "Match"),
]


def connect_to_server(driver, server, database, username, password):
    """
    connect_to_server Connects to a remote database

    :return: a cursor to navigate the db
    """
    connectionString = (
        "DRIVER=" 
        + driver  
        + ";SERVER="
        + server
        + ";DATABASE="
        + database
        + ";UID="
        + username
        + ";PWD="
        + password
    )
    cnxn = pyodbc.connect(connectionString)

    return cnxn


def to_int(el):
    """
    check_type returns el casted to int. If not applicable, returns el
    """
    if isinstance(el, str):
        try:
            return int(el)
        except (ValueError, TypeError):
            return el


def main():

    cnxn = connect_to_server(
        driver="{ODBC Driver 17 for SQL Server}",
        server="lds.di.unipi.it",
        database="GROUP_22_DB",
        username="Group_22",
        password="VY3XFPQF",
    )
    with cnxn.cursor() as cursor:

        for tab_csv, tab_name in TABLES:
            with open(tab_csv) as open_csv:

                file = reader(open_csv)
                header = next(file)
                placeholders = "?"

                for _ in range(len(header) - 1):
                    placeholders += ",?"

                tablename = (
                    "[Group_22_DB].Group_22.["
                    + tab_name
                    + "] ("
                    + ", ".join(header)
                    + ")"
                )

                query = (
                    "INSERT INTO " + tablename + " VALUES " + "(" + placeholders + ")"
                )

                print("Inserting data into" + tab_name + "...")
                for line in file:
                    new_line = []
                    for el in line:
                        # torney_revenue è l'unico valore float.
                        # Si controlla quindi il nome della tabella e se el == al valore nella
                        # colonna tourney_revenue
                        if (tab_name == "Tournament") and (el == line[-1]):
                            line_ = float(el)
                            new_line.append(line_)
                        else:
                            line_ = to_int(el)
                            new_line.append(line)
                    cursor.execute(query, tuple(new_line))

        cursor.commit()
        print("Done! :)")
    cnxn.close()


if __name__ == "__main__":
    main()
