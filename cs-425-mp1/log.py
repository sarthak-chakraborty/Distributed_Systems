import logging
import lorem
import random

MACHINE_NUM = 4
LINES = 300000

logging.basicConfig(filename=f"machine.{MACHINE_NUM}.log",
                    # filemode='a',
                    format='[%(asctime)s | %(levelname)s]: %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(f'machine.{MACHINE_NUM}.log')

# Starting Few Lines
logging.info("Welcome to CS425 Distributed Systems Class")
logging.info(f"This is machine {MACHINE_NUM} and it is used for distributed logging and debugging")
logging.info("This log will be generated automatically")

known_message_list = [("E", "This is an error message"),
                  ("W", "The system is experience high latency and error rate"),
                  ("E", "Exception: Divide by 0. Segmantation Fault"),
                  ("C", "System FAILED!! ABORT!!"),
                  ("I", "Computation Success!!")]


# Generate LINES number of logging lines in the log file.
# For every 10th iteration, generate a known message from the known_message_list, or generate lorem ipsum with DEBUG level

known_after_every = 18

for i in range(LINES):
    print(i, end = '\r')
    if i % known_after_every == 0:
        
        for k in range(1,6):
            if (i - (k*known_after_every)) % (5*known_after_every) == 0:
                known_message = known_message_list[k-1]

                message_level = known_message[0]
                message = known_message[1]
                    
                if message_level == "I":
                    logging.info(message)
                elif message_level == "E":
                    logging.error(message)
                elif message_level == "W":
                    logging.warning(message)
                elif message_level == "C":
                    logging.critical(message)
                
                break

    else:
        # If random.choice = 1, generate a sentence, else generate a paragraph
        if random.choice([0,1]) == 1:
            message = lorem.sentence()
        else:
            message = lorem.paragraph()
        
        logging.debug(message)