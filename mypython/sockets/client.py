import mysocket

def main():
    try:
        mysocket.connect()
    except KeyboardInterrupt:
        example.stop()

if __name__ == '__main__':
    main()

