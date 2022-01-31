from migen import *
from math import ceil, log2
from artiq.gateware.rtio import rtlink


class RTServoCtrl(Module):
    """Per channel RTIO control interface"""
    def __init__(self, ctrl, ctrl_reftime):
        self.rtlink = rtlink.Interface(
            rtlink.OInterface(
                data_width=max(len(ctrl.profile) + 3,
                               len(ctrl_reftime.sysclks_fine)),
                address_width=1)
            )

        # # #

        sel_ref = self.rtlink.o.address[0]
        self.comb += [
                ctrl.stb.eq(self.rtlink.o.stb & ~sel_ref),
                self.rtlink.o.busy.eq(0),
                ctrl_reftime.stb.eq(self.rtlink.o.stb & sel_ref),
        ]
        ctrl_cases = {
            0: Cat(ctrl.en_out, ctrl.en_iir, ctrl.en_pt, ctrl.profile).eq(
                            self.rtlink.o.data),
            1: ctrl_reftime.sysclks_fine.eq(self.rtlink.o.data),
        }
        self.sync.rio_phy += [
                If(self.rtlink.o.stb, Case(self.rtlink.o.address, ctrl_cases))
        ]


def _eq_sign_extend(t, s):
    """Assign target signal `t` from source `s`, sign-extending `s` to the
    full width.
    """
    return t.eq(Cat(s, Replicate(s[-1], len(t) - len(s))))


class RTServoMem(Module):
    """All-channel all-profile coefficient and state RTIO control
    interface.

    Servo internal addresses are internal_address_width wide, which is
    typically longer than the 8-bit RIO address space. We pack the overflow
    onto the RTIO data word after the data.

    Servo address space (from MSB):
      - we (1 bit)
      - sel_coeff (1 bit)
        If selected, the coefficient memory location is 
            addressed by all the lower bits excluding the LSB (high_coeff). 
          - high_coeff (1 bit) selects between the upper and lower halves of that 
            memory location.
        Else (if ~sel_coeff), the following bits are:
          - sel (2 bits) selects between the following memory locations:

                 destination    |  sel  |  sel_coeff   |
                ----------------|-------|--------------|
                 IIR coeff mem  |   -   |       1      |
                 DDS delay mem  |   1   |       0      |
                 IIR state mem  |   2   |       0      |
                 config (write) |   3   |       0      |
                 status (read)  |   3   |       0      |

          - IIR state memory address

    Values returned to the user on the Python side of the RTIO interface are
    32 bit, so we sign-extend all values from w.coeff to that width. This works
    (instead of having to decide whether to sign- or zero-extend per address), as
    all unsigned values are less wide than w.coeff.
    """
    def __init__(self, w, servo, io_update_phys):
        m_coeff = servo.iir.m_coeff.get_port(write_capable=True,
                mode=READ_FIRST,
                we_granularity=w.coeff, clock_domain="rio")
        assert len(m_coeff.we) == 2
        m_state = servo.iir.m_state.get_port(write_capable=True,
                # mode=READ_FIRST,
                clock_domain="rio")
        self.specials += m_state, m_coeff
        # width of channel address 
        w_channel = ceil(log2(len(servo.iir.dds)))

        # just expose the w.coeff (18) MSBs of state
        assert w.state >= w.coeff
        # ensure that we can split the coefficient storage correctly
        assert len(m_coeff.dat_w) == 2*w.coeff
        # ensure that the DDS word data fits into the coefficient mem
        assert w.coeff >= w.word
        # ensure all unsigned values will be zero-extended on sign extension
        assert w.word < w.coeff
        assert 8 + w.dly < w.coeff

        # coeff, profile, channel, 2 mems, rw
        internal_address_width = 3 + w.profile + w_channel + 1 + 1
        rtlink_address_width = min(8, internal_address_width)
        overflow_address_width = internal_address_width - rtlink_address_width
        self.rtlink = rtlink.Interface(
            rtlink.OInterface(
                data_width=overflow_address_width + w.coeff,
                address_width=rtlink_address_width,
                enable_replace=False),
            rtlink.IInterface(
                data_width=32,
                timestamped=False)
            )

        # # #

        config = Signal(w.coeff, reset=0)
        status = Signal(len(self.rtlink.i.data))
        pad = Signal(6)
        self.comb += [
                Cat(servo.start).eq(config),
                status.eq(Cat(servo.start, servo.done, pad,
                    [_.clip for _ in servo.iir.ctrl]))
        ]

        assert len(self.rtlink.o.address) + len(self.rtlink.o.data) - w.coeff == (
                1 +  # we
                1 +  # sel_coeff
                1 +  # high_coeff
                len(m_coeff.adr))
        # ensure that we can fit config/io_dly/status into the state address space
        assert len(self.rtlink.o.address) + len(self.rtlink.o.data) - w.coeff >= (
                1 +  # we
                1 +  # sel_coeff
                2 +  # sel
                len(m_state.adr))
        # ensure that IIR state mem addresses are at least 2 bits less wide than
        # IIR coeff mem addresses to ensure we can fit SEL after the state mem
        # address and before the SEL_COEFF bit.
        assert w.profile + w_channel >= 4

        internal_address = Signal(internal_address_width)
        self.comb += internal_address.eq(Cat(self.rtlink.o.address,
                                             self.rtlink.o.data[w.coeff:]))

        coeff_data = Signal(w.coeff)
        self.comb += coeff_data.eq(self.rtlink.o.data[:w.coeff])

        we = internal_address[-1]
        sel_coeff = internal_address[-2]
        sel1 = internal_address[-3]
        sel0 = internal_address[-4]
        high_coeff = internal_address[0]
        sel = Signal(2)
        self.comb += [
                self.rtlink.o.busy.eq(0),
                sel.eq(Mux(sel_coeff, 0, Cat(sel0, sel1))),
                m_coeff.adr.eq(internal_address[1:]),
                m_coeff.dat_w.eq(Cat(coeff_data, coeff_data)),
                m_coeff.we[0].eq(self.rtlink.o.stb & ~high_coeff & we & sel_coeff),
                m_coeff.we[1].eq(self.rtlink.o.stb & high_coeff & we & sel_coeff),
                m_state.adr.eq(internal_address),
                m_state.dat_w[w.state - w.coeff:].eq(self.rtlink.o.data),
                m_state.we.eq(self.rtlink.o.stb & we & (sel == 2)),
        ]

        read = Signal()
        read_high = Signal()
        read_sel = Signal(2)
        self.sync.rio += [
                If(read,
                    read.eq(0) # reset rtlink.i.stb at rio clock edge
                ),
                If(self.rtlink.o.stb,
                    read.eq(~we), 
                    read_sel.eq(sel),
                    read_high.eq(high_coeff),
                )
        ]

        # I/O update alignment delays
        ioup_dlys = Cat(*[phy.fine_ts for phy in io_update_phys])
        assert w.coeff >= len(ioup_dlys)

        self.sync.rio_phy += [
                If(self.rtlink.o.stb & we & (sel == 3),
                    config.eq(self.rtlink.o.data)
                ),
                If(read & (read_sel == 3),
                    [_.clip.eq(0) for _ in servo.iir.ctrl]
                ),
                If(self.rtlink.o.stb & we & (sel == 1),
                    ioup_dlys.eq(self.rtlink.o.data)
                ),
        ]
        read_acts = { # read return value by destination
            0: _eq_sign_extend(self.rtlink.i.data,
                Mux(read_high, m_coeff.dat_r[w.coeff:], m_coeff.dat_r[:w.coeff])),
            1: _eq_sign_extend(self.rtlink.i.data, ioup_dlys),
            2: _eq_sign_extend(self.rtlink.i.data, m_state.dat_r[w.state - w.coeff:]),
            3: _eq_sign_extend(self.rtlink.i.data, status)
        }
        self.comb += [
                self.rtlink.i.stb.eq(read),
                Case(read_sel, read_acts)
        ]
