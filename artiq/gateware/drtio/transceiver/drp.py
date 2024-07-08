from migen import *


from misoc.interconnect.csr import *



class DRP(Module, AutoCSR):
    def __init__(self, transceivers):
        # transceivers: list of GTP objects (each of which contains up to 4 channel objects)
        # the channel objects are flattened into a list, channel 0 is the first channel in the first
        # transceiver object, indexing first through the channel list on the first transceiver, then
        # the second, etc.
        #
        # Use:
        #
        # Write 1 to "enable" after GTP init is complete, before using the DRP interface
        # 
        # Wait for idle asserted
        # Load channel number into "channel"
        # Load address into "addr"
        # Write: load data into "write" - this triggers the write
        # Read: write anything to "trigger_read" - when idle again data in in "read"

        self.enable = CSRStorage(reset=0)
        self.n_channels = CSRStatus(8)
        self.channel = CSRStorage(8)

        self.addr = CSRStorage(9)
        self.write = CSRStorage(16)
        self.trigger_read = CSRStorage()
        self.read = CSRStatus(16)
        self.idle = CSRStatus()

        # # # 

        self.channels = []
        for transceiver in transceivers:
            for ch in transceiver.gtps:
                self.channels.append(ch)

        self.comb += self.n_channels.status.eq(len(self.channels))

        drpen = Signal()
        drprdy = Signal()
        drpdo = Signal(16)
        drpwe = Signal()

        cases_rdy = {}
        cases_do = {}

        for i_ch, ch in enumerate(self.channels):
            cases_rdy[i_ch] = drprdy.eq(ch.drp_rdy)
            cases_do[i_ch] = drpdo.eq(ch.drp_do)
            self.comb += [
                ch.drp_enable.eq(self.enable.storage),
                ch.drp_addr.eq(self.addr.storage),
                ch.drp_di.eq(self.write.storage),
                ch.drp_we.eq(drpwe),
                ch.drp_en.eq(drpen & (self.channel.storage == i_ch))
            ]
        self.comb += [
            Case(self.channel.storage, cases_rdy), 
            Case(self.channel.storage, cases_do), 
        ]



        fsm = FSM()
        self.submodules += fsm

        fsm.act("IDLE",
            If(self.write.re, NextState("WRITE")),
            If(self.trigger_read.re, NextState("READ")),
        )
        self.comb += self.idle.status.eq( fsm.ongoing("IDLE") )

        fsm.act("WRITE",
            drpen.eq(1),
            drpwe.eq(1),
            NextState("WAIT_DONE")
        )

        fsm.act("READ",
            drpen.eq(1),
            NextState("WAIT_DONE")
        )

        fsm.act("WAIT_DONE",
            If(drprdy, NextState("IDLE"))
        )

        self.sync += If(drprdy, self.read.status.eq(drpdo) )