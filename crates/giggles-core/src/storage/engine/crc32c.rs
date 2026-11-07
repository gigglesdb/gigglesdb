const fn table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0x82f6_3b78 } else { 0 };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

const TABLE: [u32; 256] = table();

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_parts(&[bytes])
}

pub(crate) fn crc32c_parts(parts: &[&[u8]]) -> u32 {
    let mut crc = !0u32;
    for part in parts {
        for byte in *part {
            crc = TABLE[((crc as u8) ^ byte) as usize] ^ (crc >> 8);
        }
    }
    !crc
}
