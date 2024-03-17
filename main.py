from mongo_operations import main as mongo_main
from mysql_operations import main as mysql_main

def main():
    
    # #Call MongoDB script
    print("Running MongoDB script...")
    mongo_main()

    #Call MySQL script
    print("Running MySQL script...")
    mysql_main()

if __name__ == "__main__":
    main()
