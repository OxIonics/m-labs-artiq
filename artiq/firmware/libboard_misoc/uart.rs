use csr;

pub fn set_speed(rate: u32) {
    unsafe {
        let tuning_word = (rate as u64) * (1 << 32) / (csr::CONFIG_CLOCK_FREQUENCY as u64);
        csr::uart_phy::tuning_word_write(tuning_word as u32);
    }
}

pub fn read() -> Result<u8,()> {
    unsafe {
        if csr::uart::rxempty_read() != 0 {
            return Err(());
        }

        let c = csr::uart::rxtx_read();
        csr::uart::ev_pending_write(0x2);
        Ok(c)
    }
}