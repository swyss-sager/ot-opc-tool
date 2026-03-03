# ---------------------------------------------------------------------------
# Entry point — select which OPC UA reader to run via boolean flags.
# ---------------------------------------------------------------------------

from src.app_opc_reader.logic.process_historian_runner import ProcessHistorianRunner
from src.app_opc_reader.logic.wincc_opc_ua_reader import WinCCOpcUaReader

if __name__ == "__main__":

    RUN_WINCC = False
    RUN_PH = True

    # -- WinCC ---------------------------------------------------------------
    if RUN_WINCC:
        reader = WinCCOpcUaReader("opc.tcp://VMSRV01:4862")
        try:
            reader.connect()
            reader.read_tag_wincc(
                r"ns=1;s=v|09_SaWasser\KomWinCC_MWA_560B5",
                history_minutes_back=240,
            )
        finally:
            reader.disconnect()

    # -- Process Historian ---------------------------------------------------
    if RUN_PH:
        ProcessHistorianRunner().run(debug=True)
