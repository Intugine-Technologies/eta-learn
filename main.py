from srcdest import get_data_db
import time

if __name__ == '__main__':
    now = time.time()
    get_data_db()
    print("Total Time taken = ", (time.time() - now), " seconds ------")
    # write_to_db(get_eta())
    # write_to_file()