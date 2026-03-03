import asyncio

from src.app.logic.process_historian_reader import ProcessHistorianReader
from src.app.logic.wincc_opc_ua_reader import WinCCOpcUaReader

if __name__ == "__main__":
    do_winCC:bool = False
    do_PH:bool = True
    # =========================
    # WinCC
    # =========================
    if do_winCC:
        endpoint_wincc  = "opc.tcp://VMSRV01:4862"
        node_id_wincc   = r"ns=1;s=v|09_SaWasser\KomWinCC_MWA_560B5"

        r = WinCCOpcUaReader(endpoint_wincc)
        try:
            r.connect()
            r.read_tag_wincc(node_id_wincc, history_minutes_back=240)
        finally:
            r.disconnect()


    # =========================
    # PH
    # =========================

    if do_PH:
        from src.app.logic.process_historian_runner import ProcessHistorianRunner
        ProcessHistorianRunner().run(debug=True)



