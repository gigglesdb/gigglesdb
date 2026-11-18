// glibc 2.41 gamma coefficients. See gamma.rs for copyright notices.
pub const A0: f64 = 7.72156649015328655494e-02;
pub const A1: f64 = 3.22467033424113591611e-01;
pub const A2: f64 = 6.73523010531292681824e-02;
pub const A3: f64 = 2.05808084325167332806e-02;
pub const A4: f64 = 7.38555086081402883957e-03;
pub const A5: f64 = 2.89051383673415629091e-03;
pub const A6: f64 = 1.19270763183362067845e-03;
pub const A7: f64 = 5.10069792153511336608e-04;
pub const A8: f64 = 2.20862790713908385557e-04;
pub const A9: f64 = 1.08011567247583939954e-04;
pub const A10: f64 = 2.52144565451257326939e-05;
pub const A11: f64 = 4.48640949618915160150e-05;
pub const TC: f64 = 1.46163214496836224576e+00;
pub const TF: f64 = -1.21486290535849611461e-01;
pub const TT: f64 = -3.63867699703950536541e-18;
pub const T0: f64 = 4.83836122723810047042e-01;
pub const T1: f64 = -1.47587722994593911752e-01;
pub const T2: f64 = 6.46249402391333854778e-02;
pub const T3: f64 = -3.27885410759859649565e-02;
pub const T4: f64 = 1.79706750811820387126e-02;
pub const T5: f64 = -1.03142241298341437450e-02;
pub const T6: f64 = 6.10053870246291332635e-03;
pub const T7: f64 = -3.68452016781138256760e-03;
pub const T8: f64 = 2.25964780900612472250e-03;
pub const T9: f64 = -1.40346469989232843813e-03;
pub const T10: f64 = 8.81081882437654011382e-04;
pub const T11: f64 = -5.38595305356740546715e-04;
pub const T12: f64 = 3.15632070903625950361e-04;
pub const T13: f64 = -3.12754168375120860518e-04;
pub const T14: f64 = 3.35529192635519073543e-04;
pub const U0: f64 = -7.72156649015328655494e-02;
pub const U1: f64 = 6.32827064025093366517e-01;
pub const U2: f64 = 1.45492250137234768737e+00;
pub const U3: f64 = 9.77717527963372745603e-01;
pub const U4: f64 = 2.28963728064692451092e-01;
pub const U5: f64 = 1.33810918536787660377e-02;
pub const V1: f64 = 2.45597793713041134822e+00;
pub const V2: f64 = 2.12848976379893395361e+00;
pub const V3: f64 = 7.69285150456672783825e-01;
pub const V4: f64 = 1.04222645593369134254e-01;
pub const V5: f64 = 3.21709242282423911810e-03;
pub const S0: f64 = -7.72156649015328655494e-02;
pub const S1: f64 = 2.14982415960608852501e-01;
pub const S2: f64 = 3.25778796408930981787e-01;
pub const S3: f64 = 1.46350472652464452805e-01;
pub const S4: f64 = 2.66422703033638609560e-02;
pub const S5: f64 = 1.84028451407337715652e-03;
pub const S6: f64 = 3.19475326584100867617e-05;
pub const R1: f64 = 1.39200533467621045958e+00;
pub const R2: f64 = 7.21935547567138069525e-01;
pub const R3: f64 = 1.71933865632803078993e-01;
pub const R4: f64 = 1.86459191715652901344e-02;
pub const R5: f64 = 7.77942496381893596434e-04;
pub const R6: f64 = 7.32668430744625636189e-06;
pub const W0: f64 = 4.18938533204672725052e-01;
pub const W1: f64 = 8.33333333333329678849e-02;
pub const W2: f64 = -2.77777777728775536470e-03;
pub const W3: f64 = 7.93650558643019558500e-04;
pub const W4: f64 = -5.95187557450339963135e-04;
pub const W5: f64 = 8.36339918996282139126e-04;
pub const W6: f64 = -1.63092934096575273989e-03;
pub const LGAMMA_ZEROS: [f64; 104] = [
    f64::from_bits(0xc003a7fc9600f86c),
    f64::from_bits(0xbc855f64f98af8d0),
    f64::from_bits(0xc005fb410a1bd901),
    f64::from_bits(0x3c9a19a96d2e6f85),
    f64::from_bits(0xc009260dbc9e59af),
    f64::from_bits(0xbcaf717cd335a7b3),
    f64::from_bits(0xc00fa471547c2fe5),
    f64::from_bits(0xbc770d4561291237),
    f64::from_bits(0xc010284e78599581),
    f64::from_bits(0x3cae78c1e9e43cfe),
    f64::from_bits(0xc013f7577a6eeafd),
    f64::from_bits(0x3ca5de5eab7f12cf),
    f64::from_bits(0xc014086a57f0b6d9),
    f64::from_bits(0xbc895262b72ca9ca),
    f64::from_bits(0xc017fe92f591f40d),
    f64::from_bits(0xbcb7dd4ed62cbd32),
    f64::from_bits(0xc018016b25897c8d),
    f64::from_bits(0x3c927e0f49a4ba72),
    f64::from_bits(0xc01bffcbf76b86f0),
    f64::from_bits(0x3c6853b29347b806),
    f64::from_bits(0xc01c0033fdedfe1f),
    f64::from_bits(0x3cb20bb7d2324678),
    f64::from_bits(0xc01ffff97f8159cf),
    f64::from_bits(0xbc8e54f415a91586),
    f64::from_bits(0xc02000034028b3f9),
    f64::from_bits(0xbcbf60cb3cec1ced),
    f64::from_bits(0xc021ffffa3884bd0),
    f64::from_bits(0xbcaff90c9d2ae925),
    f64::from_bits(0xc02200005c7768fb),
    f64::from_bits(0xbc9b5b610ffb70d4),
    f64::from_bits(0xc023fffff6c0d7c0),
    f64::from_bits(0x3cc197cea8c42d7d),
    f64::from_bits(0xc0240000093f2777),
    f64::from_bits(0xbcb927b45d95e154),
    f64::from_bits(0xc025ffffff28cdd4),
    f64::from_bits(0x3cac9924a65aa486),
    f64::from_bits(0xc026000000d7322a),
    f64::from_bits(0xbcc8aecb2d37ff52),
    f64::from_bits(0xc027ffffffee1127),
    f64::from_bits(0xbc9ce1f7906b30f5),
    f64::from_bits(0xc02800000011eed9),
    f64::from_bits(0x3ca19d5307e1fb5e),
    f64::from_bits(0xc029fffffffe9edc),
    f64::from_bits(0x3cc84f40342d001c),
    f64::from_bits(0xc02a000000016124),
    f64::from_bits(0xbcc84e03341ee8dd),
    f64::from_bits(0xc02bffffffffe6c7),
    f64::from_bits(0x3ccd2a30f3dae0fb),
    f64::from_bits(0xc02c000000001939),
    f64::from_bits(0xbccd2a2f4a73af63),
    f64::from_bits(0xc02dfffffffffe52),
    f64::from_bits(0x3ccfcf9ccfd8867e),
    f64::from_bits(0xc02e0000000001ae),
    f64::from_bits(0xbccfcf9ccde87210),
    f64::from_bits(0xc02fffffffffffe5),
    f64::from_bits(0xbca80c18cc43ea26),
    f64::from_bits(0xc03000000000000d),
    f64::from_bits(0xbcdcfe7ce6768509),
    f64::from_bits(0xc030ffffffffffff),
    f64::from_bits(0xbccab4e23f3d49f1),
    f64::from_bits(0xc031000000000001),
    f64::from_bits(0x3ccab4e23f3d4bbc),
    f64::from_bits(0xc032000000000000),
    f64::from_bits(0x3ca6827863b97d9a),
    f64::from_bits(0xc032000000000000),
    f64::from_bits(0xbca6827863b97d95),
    f64::from_bits(0xc033000000000000),
    f64::from_bits(0x3c62f49b46814157),
    f64::from_bits(0xc033000000000000),
    f64::from_bits(0xbc62f49b46814157),
    f64::from_bits(0xc034000000000000),
    f64::from_bits(0x3c1e542ba4020225),
    f64::from_bits(0xc034000000000000),
    f64::from_bits(0xbc1e542ba4020225),
    f64::from_bits(0xc035000000000000),
    f64::from_bits(0x3bd71b8ef6dcf572),
    f64::from_bits(0xc035000000000000),
    f64::from_bits(0xbbd71b8ef6dcf572),
    f64::from_bits(0xc036000000000000),
    f64::from_bits(0x3b90ce396db7f853),
    f64::from_bits(0xc036000000000000),
    f64::from_bits(0xbb90ce396db7f853),
    f64::from_bits(0xc037000000000000),
    f64::from_bits(0x3b4761b41316381a),
    f64::from_bits(0xc037000000000000),
    f64::from_bits(0xbb4761b41316381a),
    f64::from_bits(0xc038000000000000),
    f64::from_bits(0x3aff2cf01972f578),
    f64::from_bits(0xc038000000000000),
    f64::from_bits(0xbaff2cf01972f578),
    f64::from_bits(0xc039000000000000),
    f64::from_bits(0x3ab3f3ccdd165fa9),
    f64::from_bits(0xc039000000000000),
    f64::from_bits(0xbab3f3ccdd165fa9),
    f64::from_bits(0xc03a000000000000),
    f64::from_bits(0x3a688e85fc6a4e5a),
    f64::from_bits(0xc03a000000000000),
    f64::from_bits(0xba688e85fc6a4e5a),
    f64::from_bits(0xc03b000000000000),
    f64::from_bits(0x3a1d1ab1c2dccea3),
    f64::from_bits(0xc03b000000000000),
    f64::from_bits(0xba1d1ab1c2dccea3),
    f64::from_bits(0xc03c000000000000),
    f64::from_bits(0x39d0a18a2635085d),
];
pub const LGAMMA_COEFF: [f64; 12] = [
    f64::from_bits(0x3fb5555555555555),
    f64::from_bits(0xbf66c16c16c16c17),
    f64::from_bits(0x3f4a01a01a01a01a),
    f64::from_bits(0xbf43813813813814),
    f64::from_bits(0x3f4b951e2b18ff23),
    f64::from_bits(0xbf5f6ab0d9993c7d),
    f64::from_bits(0x3f7a41a41a41a41a),
    f64::from_bits(0xbf9e4286cb0f5398),
    f64::from_bits(0x3fc6fe96381e0680),
    f64::from_bits(0xbff6476701181f3a),
    f64::from_bits(0x402ace44322ce006),
    f64::from_bits(0xc0639b2525cccc1b),
];
pub const POLY_COEFF: [f64; 101] = [
    f64::from_bits(0xbff0b71c5c54d42f),
    f64::from_bits(0xbfe8e743b80be6eb),
    f64::from_bits(0xbfbec84140851911),
    f64::from_bits(0xbfec6f93b44708fd),
    f64::from_bits(0xbfb03cd87cdc0ac6),
    f64::from_bits(0xbfed5d35db9c25dc),
    f64::from_bits(0x3fa362342f019fa9),
    f64::from_bits(0xbfede4bf88c17bd6),
    f64::from_bits(0x3fc374c2630e5489),
    f64::from_bits(0xbfef07fd438da6a0),
    f64::from_bits(0x3fd1d832322425d6),
    f64::from_bits(0xbfee5261121afacf),
    f64::from_bits(0xbfe94b9fbc0a9d55),
    f64::from_bits(0x3fcce4f07ef5ecce),
    f64::from_bits(0xbff02a5ad35619d9),
    f64::from_bits(0x3fe2dd3622cf82c8),
    f64::from_bits(0xbff4d8332eba090a),
    f64::from_bits(0x3ff1c0c94b1b2b60),
    f64::from_bits(0xbffc9a70d138c74e),
    f64::from_bits(0x3ffd7d9cf1d4c196),
    f64::from_bits(0xc0048fdfa66b55d6),
    f64::from_bits(0x4007b3a8fba5c7fc),
    f64::from_bits(0xc00f0ddbd84f1f3b),
    f64::from_bits(0xbfeafa51aa0bac30),
    f64::from_bits(0xbfecd2c93460812b),
    f64::from_bits(0x3fe61ae94504f9ac),
    f64::from_bits(0xbff924b09228a86e),
    f64::from_bits(0x3ffd49b12bcf6175),
    f64::from_bits(0xc00844c5da98698a),
    f64::from_bits(0x401081e9afa3f693),
    f64::from_bits(0xc018e7bbd5b53a40),
    f64::from_bits(0x4021c5c8559799d9),
    f64::from_bits(0xc02a1b2383cb2d4d),
    f64::from_bits(0x4032e20d7099c585),
    f64::from_bits(0xc03c4eb6691b4ca9),
    f64::from_bits(0x4044b50d08fec2ff),
    f64::from_bits(0xbfe6e9d4379fff29),
    f64::from_bits(0xbff2a82bd590c376),
    f64::from_bits(0x3ff88020f828b810),
    f64::from_bits(0xc009913cf8206bd7),
    f64::from_bits(0x40155eb2094b3a1a),
    f64::from_bits(0xc023855dba12624a),
    f64::from_bits(0x40312c132716e94c),
    f64::from_bits(0xc03ea94dfa5c0a6d),
    f64::from_bits(0x404b35b0d5ff42c6),
    f64::from_bits(0xc05833f3ad8a89b9),
    f64::from_bits(0x40657dd74128077b),
    f64::from_bits(0xc073188d9b1b288c),
    f64::from_bits(0x4081bfbba6e8a622),
    f64::from_bits(0xc08fef853266b89b),
    f64::from_bits(0xbfce88084613f5d8),
    f64::from_bits(0x3ffcd557caff7d2f),
    f64::from_bits(0x400c0cda42b69b67),
    f64::from_bits(0x401a14172eb3f108),
    f64::from_bits(0x40278365ccacf49a),
    f64::from_bits(0x40350a53a3ce6c73),
    f64::from_bits(0x4042bd6ffdd8f606),
    f64::from_bits(0x4050ac555273d003),
    f64::from_bits(0x405da633f60acf86),
    f64::from_bits(0x406a5bd9bc42eeaa),
    f64::from_bits(0x4077699a624d07b9),
    f64::from_bits(0x4084c767b0bd5dfe),
    f64::from_bits(0x409354913513582d),
    f64::from_bits(0x40a1d2c57ee72b31),
    f64::from_bits(0xbfdad7494a95ba2a),
    f64::from_bits(0x3ff28d60383da3a6),
    f64::from_bits(0x3ffdb6513ada89be),
    f64::from_bits(0x400710b88c7d4601),
    f64::from_bits(0x401114044b1944d2),
    f64::from_bits(0x40192be643d626e3),
    f64::from_bits(0x40225b6b2c7af447),
    f64::from_bits(0x402ac5806c8fb432),
    f64::from_bits(0x403379f81f6416af),
    f64::from_bits(0x403c5618b4fdb960),
    f64::from_bits(0x40449a1685795627),
    f64::from_bits(0x404ece6fab6958c3),
    f64::from_bits(0x4056ae7e475689e9),
    f64::from_bits(0xbfe148363c9e6dff),
    f64::from_bits(0x3fe9b50fa76d3b7d),
    f64::from_bits(0x3ff1474ad5c36709),
    f64::from_bits(0x3ff761ecb90c8c5c),
    f64::from_bits(0x3ffd279bff588826),
    f64::from_bits(0x400272e801fd9b54),
    f64::from_bits(0x4006abaabaab3421),
    f64::from_bits(0x400c28a95868bbab),
    f64::from_bits(0x4011484f6487284c),
    f64::from_bits(0x40155769f7f3da71),
    f64::from_bits(0x401ab3bdca6e5010),
    f64::from_bits(0x40209079843ba0cd),
    f64::from_bits(0xbfe408daccfc8d1f),
    f64::from_bits(0x3fe2e1711b9800d8),
    f64::from_bits(0x3fe551472843929a),
    f64::from_bits(0x3fea5e9a26c7f31c),
    f64::from_bits(0x3feb95354332eb6e),
    f64::from_bits(0x3fef9e1385ea8808),
    f64::from_bits(0x3ff04b1365a9adfc),
    f64::from_bits(0x3ff22b54ef213798),
    f64::from_bits(0x3ff2c52c25206bf5),
    f64::from_bits(0x3ff4aa3d798aace4),
    f64::from_bits(0x3ff5c3f278b504e3),
    f64::from_bits(0x3ff7e08292cc347b),
];
pub const GAMMA_COEFF: [f64; 6] = [
    f64::from_bits(0x3fb5555555555555),
    f64::from_bits(0xbf66c16c16c16c17),
    f64::from_bits(0x3f4a01a01a01a01a),
    f64::from_bits(0xbf43813813813814),
    f64::from_bits(0x3f4b951e2b18ff23),
    f64::from_bits(0xbf5f6ab0d9993c7d),
];
pub const EXP2_POLY: [f64; 5] = [
    f64::from_bits(0x3fe62e42fefa39ef),
    f64::from_bits(0x3fcebfbdff82c424),
    f64::from_bits(0x3fac6b08d70cf4b5),
    f64::from_bits(0x3f83b2abd24650cc),
    f64::from_bits(0x3f55d7e09b4e3a84),
];
