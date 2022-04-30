from threading import Thread
from dmm import main as dmm
from lvdt import main as lvdt
from battTemp import main as battTemp
from eventDMM import main as eventDMM
from eventLVDT import main as eventLVDT

Thread(target = dmm).start() 
Thread(target = lvdt).start()
Thread(target = battTemp).start()
Thread(target = eventDMM).start()
Thread(target = eventLVDT).start()
