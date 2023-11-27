import RPi.GPIO as GPIO
import time
import sys
import tomli
import zmq
import logging


class MWSwitch():
    """
    Sets up and manages gpio state to switch the microwave switch.
    """


    def __init__(self, gpio_pos_1, gpio_pos_2, pulse_length):
        self._gpio_1 = gpio_pos_1
        self._gpio_2 = gpio_pos_2
        self._pulse_length = pulse_length
        logging.debug(f"Configured to address pins {self._gpio_1} and {self._gpio_2} for {self._pulse_length}s.")

        GPIO.setmode(GPIO.BOARD) # type: ignore
        GPIO.setup(self._gpio_1, GPIO.OUT) # type: ignore
        GPIO.setup(self._gpio_2, GPIO.OUT) # type: ignore

    def __del__(self):
        GPIO.cleanup() # type: ignore
    
    def switch_to_1(self):
        """
        Switch to switch position 1.
        """
        logging.info("Switching to position 1...")

        GPIO.output(self._gpio_1, True) # type: ignore
        GPIO.output(self._gpio_2, False) # type: ignore

        time.sleep(self._pulse_length)

        GPIO.output(self._gpio_1, False) # type: ignore
        GPIO.output(self._gpio_2, False) # type: ignore

        logging.debug("Switched to position 1.")

    def switch_to_2(self):
        """
        Switch to switch position 2.
        """
        logging.info("Switching to position 2...")

        GPIO.output(self._gpio_1, False) # type: ignore
        GPIO.output(self._gpio_2, True) # type: ignore

        time.sleep(self._pulse_length)

        GPIO.output(self._gpio_1, False) # type: ignore
        GPIO.output(self._gpio_2, False) # type: ignore

        logging.debug("Switched to position 2.")


def parse_request(message: str, switch: MWSwitch) -> str:
    if message == "S1":
        switch.switch_to_1()
    elif message == "S2":
        switch.switch_to_2()
    else:
        return "Invalid"
    return "OK"

def main(args=sys.argv):
    # Configure logging first.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)
    root_logger.addHandler(stdout_handler)
    
    logging.info("Starting Switch Server")

    assert len(args) >= 2, "A config file is required!"
    config_file = " ".join(args[1:])

    logging.info(f"Loading config file {config_file}")
    with open(config_file, "r") as f:
        config = tomli.loads(f.read())

    switch = MWSwitch(config['pin_1'], config['pin_2'], config['pulse_length'])

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.zap_domain = b'global'
    socket.bind("tcp://*:5000")

    try:
        while True:
            message = socket.recv_string()
            socket.send_string(str(parse_request(message, switch)))
    except Exception as e:
        logging.error(f"Caught exception, shutting down: {e}")
    finally:
        logging.info("Shutting down Switch Server")

if __name__ == '__main__':
    main()
