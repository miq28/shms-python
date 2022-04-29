from threading import Thread
from dmm import main as dmm
from lvdt import main as lvdt
from batttemp import main as batttemp

Thread(target = dmm).start() 
Thread(target = lvdt).start()
Thread(target = batttemp).start()
