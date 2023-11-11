import multiprocessing
import threading as th
import time
import os
import yaml
import hashlib
import zlib

config = yaml.safe_load(open("config.yaml"))
pool_cap = config["uiprocesses"]
baseDir = config["directory"]
maxmem = config["maxmemory"] * 2**10
usedmem = 0
BYTES_TO_READ = maxmem//pool_cap
file_registry = {}
parts_registry = []
read_blocks = []
send = []
to_delete = []
file_counter = 0

mem_lock = th.Lock()
counter_lock = th.Lock()
read_blocks_lock = th.Lock()
send_lock = th.Lock()
delete_lock = th.Lock()
registry_lock = th.Lock()


class File:
    def __init__(self, uid, name, status, num_of_parts, parts):
        self.uid = uid
        self.name = name
        self.status = status
        self.num_of_parts = num_of_parts
        self.parts = parts


class FilePart:
    def __init__(self, uid, uidfile, serialnum, md5hash):
        self.uid = uid
        self.uidfile = uidfile
        self.serialnum = serialnum
        self.md5hash = md5hash


def print_help():
    print("List of available commands:")
    print("\tput <path> <name?> : Puts given named file in the file register")
    print("\tget <uid> : Gets file with given unique id from the file register")
    print("\tdelete <uid> : Deletes file with given unique id from the file register")
    print("\tlist or ls : Lists all files from the file register")
    print("\thelp : You are using it right now")
    print("\texit : Exits the program")


def put(path, name=None):
    if not os.path.exists(baseDir):
        os.mkdir(baseDir)
    global maxmem, usedmem, file_registry, file_counter, pool
    file = File(file_counter, name if name else path, "not-ready", 0, [])
    with registry_lock:
        file_registry[file_counter] = file
    file_size = os.stat(path).st_size
    file.num_of_parts = file_size // BYTES_TO_READ if file_size % BYTES_TO_READ == 0 else file_size // BYTES_TO_READ + 1
    with counter_lock:
        file_counter += 1
    part_counter = 0
    with open(path, 'rb') as reader:
        while True:              # cita dok ne dodje do kraja fajla
            if usedmem < maxmem:  # u slucaju da je RAM slobodan, cita blok i zauzima RAM
                with mem_lock:
                    usedmem += BYTES_TO_READ
                block = reader.read(BYTES_TO_READ)
                if not block:        # u slucaju da je dosao do kraja fajla salje preostale blokove na izradu i zavrsava
                    if read_blocks:
                        put_make_parts()
                    break
                with registry_lock:    # pravi odgovarajuci FilePart za procitan blok
                    filepart = FilePart(str(file.uid) + "/" + str(part_counter), file.uid, part_counter, None)
                    part_counter += 1
                    file.parts.append(filepart)
                with read_blocks_lock:
                    read_blocks.append((file, filepart, block))     # dodaje procitan blok u procitane blokove
            else:                   # u slucaju da je RAM zauzet, obradi procitane blokove
                if read_blocks:
                    put_make_parts()
        print("put finished. uid: " + str(file.uid))


def put_make_parts():
    global usedmem, read_blocks, file_registry
    with read_blocks_lock:
        result = pool.starmap(put_write_part, read_blocks)  # od procitanih blokova pravi delove i vraca hes
        read_blocks = []
    for r in result:   # lepi napravljene delove i md5hash sa odgovarajucim fajlom i delom fajla
        with registry_lock:
            file = file_registry[r[2]]
        if file.parts[r[1]].md5hash:
            continue
        file.parts[r[1]].md5hash = r[0]
        if r[1] == file.num_of_parts-1:
            file.status = "ready"
    with mem_lock:
        usedmem = 0


def put_write_part(file, filepart, block):   # procesi prave deo fajla i u njega upisuju  kompresovani blok
    global maxmem, usedmem, BYTES_TO_READ
    compressed = zlib.compress(block)
    with open(f"{baseDir}/{file.uid}_{file.name}_p{filepart.serialnum}.dat", "wb") as writer:
        writer.write(compressed)
        writer.flush()
    return hashlib.md5(block).hexdigest(), filepart.serialnum, file.uid


def get_file(uid):
    global maxmem, usedmem, file_registry, send
    with registry_lock:
        file = file_registry[int(uid)]
    if not file:
        print("File not found.")
        return
    if file.status == "ready":
        for part in file.parts:  # prolazimo kroz sve delove i redom ih saljemo na obradu uz ogranicenje memorije
            while usedmem >= maxmem:
                if send:
                    get_write_file()
                continue
            with send_lock:
                send.append((file, part))
            with mem_lock:
                usedmem += BYTES_TO_READ
        if send:
            get_write_file()
    else:
        print("File not ready.")
        return
    print(f"get finished. uid: {file.uid}")


def get_write_file():      # rekonstrujisemo trazen fajl preko obradjenih delova
    global usedmem, send, file_registry
    with send_lock:
        decompressed_parts = pool.starmap(get_read_part, send)
        send = send[len(decompressed_parts):]
    with mem_lock:
        usedmem -= BYTES_TO_READ * len(decompressed_parts)
    for dp in decompressed_parts:
        if dp[0] == -1:
            break
        with registry_lock:
            file = file_registry[dp[2]]
            if not os.path.exists("getfile"):
                os.mkdir("getFile")
        with open(f"getFile/{file.name}{str(file.uid)}", "ab") as writer:
            writer.write(dp[0])


def get_read_part(file, part):       # proces cita deo, dekompresuje, proverava validnost i salje dalje
    with open(f"{baseDir}/{file.uid}_{file.name}_p{part.serialnum}.dat", "rb") as reader:
        block = reader.read(BYTES_TO_READ)
        decompressed = zlib.decompress(block)
        if hashlib.md5(decompressed).hexdigest() != part.md5hash:
            print(f"Error in getting file {file.name} in part {part.serialnum}")
            return -1, part.serialnum, file.uid
    return decompressed, part.serialnum, file.uid


def listfiles():
    if len(file_registry) == 0:
        print("Empty.")
        return
    for key in file_registry:
        print(f"UID: {file_registry[key].uid}   NAME: {file_registry[key].name}   "
              f"PARTS: {str(len(file_registry[key].parts))}   STATUS: {file_registry[key].status}")


def delete(uid):        # prolazimo kroz sve delove fajla brisemo ih, nakon toga brisemo i sam fajl
    global file_registry, to_delete
    with registry_lock:
        file = file_registry[int(uid)]
        if not file:
            print("No such file.")
            return
    if file.status == "ready":
        file.status = "not-ready"
        for part in file.parts:
            with delete_lock:
                to_delete.append((file, part))
        with delete_lock:
            pool.starmap(delete_part, to_delete)
            to_delete = []
        with registry_lock:
            file.parts.clear()
            del file_registry[file.uid]
            del file
            print("File deleted.")
    else:
        print("File not ready.")
        return


def delete_part(file, part):  # proces brise deo fajla
    os.remove(f"{baseDir}/{file.uid}_{file.name}_p{part.serialnum}.dat")
    return part.serialnum


if __name__ == '__main__':      # posmatramo korisnikov unos, pravi se tred koji izvrsava trazenu funkcionalnost
    threads = []
    pool = multiprocessing.Pool(pool_cap)
    print("type help for list of commands.")
    while True:
        user_input = input("$: ")
        args = user_input.split()
        if len(args) == 1:
            if user_input == "exit":
                break
            elif user_input == "list" or user_input == "ls":
                target_function = listfiles
            elif user_input == "help":
                target_function = print_help
            else:
                print("invalid command or args, type help for list of commands")
                continue
            t = th.Thread(target=target_function, args=())
        elif len(args) == 2:
            if args[0] == "get":
                target_function = get_file
            elif args[0] == "delete" or args[0] == "del":
                target_function = delete
            elif args[0] == "put":
                target_function = put
            else:
                print("invalid command or args, type help for list of commands")
                continue
            try:
                if args[0] != "put":
                    given_uid = int(args[1])
                else:
                    given_uid = args[1]
            except ValueError:
                print("invalid args, type help for list of commands")
                continue
            t = th.Thread(target=target_function, args=(given_uid,))
        elif len(args) == 3 and args[0] == "put":
            target_function = put
            t = th.Thread(target=target_function, args=(args[1], args[2]))
        else:
            print("invalid command, type help for list of commands")
            continue
        threads.append(t)
        t.start()
        time.sleep(0.2)
    for t in threads:       # cekamo da svi tredovi i procesi zavrse sa izvrsavanjem pre nego sto ugasimo program
        t.join()
    pool.close()
    pool.join()
    print("successfully exited.")
