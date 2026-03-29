from bibmap.db.manager import set_db_connection

TEST_DOI = "10.1103/physreve.87.032113"


def main():
    conn = set_db_connection()
    print("Connection established")


if __name__ == "__main__":
    main()
