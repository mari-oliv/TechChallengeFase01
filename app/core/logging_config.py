import logging

def logging_config():
    """
    Padroniza o formato do logging.
    """
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s - %(levelname)s - %(message).200s",
        datefmt='%m/%d/%Y %I:%M:%S %p',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(".logs")
        ]
    )