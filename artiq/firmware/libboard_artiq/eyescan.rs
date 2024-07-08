use super::drp;


const ES_CONTROL: u16 = 0x3D;
const ES_PRESCALE_AND_VERT_OFFSET: u16 = 0x3b;
const ES_HORIZ_OFFSET: u16 = 0x3C;
const ES_CONTROL_STATUS: u16 = 0x153; // datasheet says 0x153, but example uses 0x151
const ES_SAMPLE_COUNT: u16 = 0x152;
const ES_ERROR_COUNT: u16 = 0x151;


pub fn init() {
    drp::set_enable(true);

    let n_ch = drp::num_channels();

    info!("Eyescan init - {} channels", n_ch);

    for ch in 0..n_ch {
        drp::set_channel(ch);

        let mut r = drp::read(ES_CONTROL);
        r |= 1<<8;
        r &= !0x3f;
        drp::write(ES_CONTROL, r);

        // info!("{}: es_control_status: {:#04x}", ch, drp::read(ES_CONTROL_STATUS));
    }

    info!("done");
}


fn eye_sample(channel: u8, vert_offset: u16, horiz_offset: u16, count_prescale: u8) -> (u16, u16) {
    drp::set_channel(channel);
    drp::write(ES_PRESCALE_AND_VERT_OFFSET, ((count_prescale as u16)<<11) | vert_offset);
    drp::write(ES_HORIZ_OFFSET, horiz_offset);

    // Start BER measurement
    let mut r = drp::read(ES_CONTROL);
    r &= !0x3f;
    r |= 1;
    drp::write(ES_CONTROL, r);

    // Busy-wait until we hit the end state
    while drp::read(ES_CONTROL_STATUS) & 0x1 != 1 {}

    let sample_count: u16 = drp::read(ES_SAMPLE_COUNT);
    let error_count: u16 = drp::read(ES_ERROR_COUNT);

    // Disable "run" to go back to wait state
    r &= !0x3f;
    drp::write(ES_CONTROL, r);

    (error_count, sample_count)
}


pub fn eye_scan(channel: u8, vert_steps: i16, horiz_steps: i16, count_prescale: u8) {
    info!("Running eye scan on ch {} with {} vert steps, {} horiz steps, {} prescale ...", channel, vert_steps, horiz_steps, count_prescale);

    let mut n_x : i16;
    let mut n_y : i16;
    let mut horiz_offset: u16;
    let mut vert_offset: u16;
    let mut error_count: u16 = 0;
    let mut sample_count: u16 = 0;

    for y in -vert_steps..vert_steps+1 {
        n_y = 128/vert_steps * y;
        // vert_offset format: [6:0]: Offset magnitude, [7]: Offset sign (1 is negative, 0 is positive)
        vert_offset = (n_y.abs() as u16) | (if n_y < 0 {1<<7} else {0});

        for x in -horiz_steps..horiz_steps+1 {
            n_x = 128/horiz_steps * x;
            // horiz_offset format: [10:0]: Phase offset (two's complement), [11]: Phase unification. set to 0 for positive counts, 1 for all negative
            horiz_offset = (n_x as u16) & 0xfff;

            let (error_count, sample_count) = eye_sample(channel, vert_offset, horiz_offset, count_prescale);

            info!("y = {}, x = {}, errs = {}, samples = {}", n_y, n_x, error_count, sample_count);
        }
    }
}