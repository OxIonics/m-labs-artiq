use board_misoc::csr;


pub fn num_channels() -> u8 {
    unsafe{
        csr::drp::n_channels_read()
    }
}


pub fn set_channel(channel: u8) {
    unsafe{
        csr::drp::channel_write(channel);
    }
}

pub fn set_enable(enable: bool) {
    unsafe{
        csr::drp::enable_write(enable as u8);
    }
}

pub fn write(addr: u16, data: u16) {
    unsafe {
        csr::drp::addr_write(addr);
        csr::drp::write_write(data);
        while csr::drp::idle_read() == 0 {}
    }
}

pub fn read(addr: u16) -> u16 {
    unsafe {
        csr::drp::addr_write(addr);
        csr::drp::trigger_read_write(0);
        while csr::drp::idle_read() == 0 {}
        csr::drp::read_read()
    }
}
