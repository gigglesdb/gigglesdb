// IBM Accurate Mathematical Library, Rust port of Debian glibc 2.41-12+deb13u4.
// Copyright (C) 2001-2025 Free Software Foundation, Inc.
// SPDX-License-Identifier: LGPL-2.1-or-later
// This is free software, distributed without warranty; see the GNU LGPL 2.1 or later.
use super::branred::branred;
use super::{hi, lo};
pub fn tan(mut x: f64) -> f64 {
    // Specify NaN payload/sign explicitly rather than relying on host arithmetic.
    if x.is_nan() {
        return f64::from_bits(x.to_bits() | (1_u64 << 51));
    }
    if x.is_infinite() {
        return f64::from_bits(0xfff8000000000000);
    }

    const d3: f64 = f64::from_bits(0x3fd5555555555555);
    const d5: f64 = f64::from_bits(0x3fc11111111107c6);
    const d7: f64 = f64::from_bits(0x3faba1ba1cdb8745);
    const d9: f64 = f64::from_bits(0x3f9664ed49cfc666);
    const d11: f64 = f64::from_bits(0x3f82385a3cf2e4ea);
    const e0: f64 = f64::from_bits(0x3fd5555555554dbd);
    const e1: f64 = f64::from_bits(0x3fc11112e0a6b45f);
    const mfftnhf: f64 = f64::from_bits(0xc02f000000000000);
    const g1: f64 = f64::from_bits(0x3e4b096c00000000);
    const g2: f64 = f64::from_bits(0x3faf212d00000000);
    const g3: f64 = f64::from_bits(0x3fe92f1a00000000);
    const g4: f64 = f64::from_bits(0x4039000000000000);
    const g5: f64 = f64::from_bits(0x4197d78400000000);
    const gy2: f64 = f64::from_bits(0x3faf212d00000000);
    const mp1: f64 = f64::from_bits(0x3ff921fb58000000);
    const mp2: f64 = f64::from_bits(0xbe4dde973c000000);
    const mp3: f64 = f64::from_bits(0xbc8cb3b399d747f2);
    const pp3: f64 = f64::from_bits(0xbc8cb3b398000000);
    const pp4: f64 = f64::from_bits(0xbacd747f23e32ed7);
    const hpinv: f64 = f64::from_bits(0x3fe45f306dc9c883);
    const toint: f64 = f64::from_bits(0x4338000000000000);
    static xfg: [[f64; 4]; 186] = [
        [
            f64::from_bits(0x3fb000001e519d60),
            f64::from_bits(0x3fb0055796c4e240),
            f64::from_bits(0x402ff554628127b7),
            f64::from_bits(0xbb9a1dee9e355b06),
        ],
        [
            f64::from_bits(0x3fb100001b1a7010),
            f64::from_bits(0x3fb10668aab892b7),
            f64::from_bits(0x402e12c7be3fdf74),
            f64::from_bits(0x3ba89234037da741),
        ],
        [
            f64::from_bits(0x3fb200002505e350),
            f64::from_bits(0x3fb2079bff547824),
            f64::from_bits(0x402c65c5de853633),
            f64::from_bits(0x3bb7486ee9614250),
        ],
        [
            f64::from_bits(0x3fb2fffffcdc4252),
            f64::from_bits(0x3fb308f35eb16c68),
            f64::from_bits(0x402ae5dae56be74f),
            f64::from_bits(0xbb82c72691a23034),
        ],
        [
            f64::from_bits(0x3fb3ffffe3ff849f),
            f64::from_bits(0x3fb40a71154999cc),
            f64::from_bits(0x40298c43046b7352),
            f64::from_bits(0x3b9aceaf3843738f),
        ],
        [
            f64::from_bits(0x3fb4ffffedc9590f),
            f64::from_bits(0x3fb50c17429bdd80),
            f64::from_bits(0x4028538491b5d674),
            f64::from_bits(0xbbc1d02db4403d22),
        ],
        [
            f64::from_bits(0x3fb6000000ee83f7),
            f64::from_bits(0x3fb60de7da80cc21),
            f64::from_bits(0x40273724ef21a2a7),
            f64::from_bits(0xbb95e53c72523ffd),
        ],
        [
            f64::from_bits(0x3fb6ffffeb05ea41),
            f64::from_bits(0x3fb70fe4b8c51bea),
            f64::from_bits(0x40263370fae562ff),
            f64::from_bits(0xbb99ad0e8ffe0626),
        ],
        [
            f64::from_bits(0x3fb7ffffdc0515f7),
            f64::from_bits(0x3fb812101db54498),
            f64::from_bits(0x402545530e7eab5c),
            f64::from_bits(0xbb914c87d62ed686),
        ],
        [
            f64::from_bits(0x3fb8ffffe384d7ab),
            f64::from_bits(0x3fb9146c2a8d3727),
            f64::from_bits(0x40246a33fd57f3fd),
            f64::from_bits(0xbbbbda8d5381e06d),
        ],
        [
            f64::from_bits(0x3fb9ffffe4832347),
            f64::from_bits(0x3fba16fad50e1050),
            f64::from_bits(0x40239fe2c5537a96),
            f64::from_bits(0x3bc7f695c111eabb),
        ],
        [
            f64::from_bits(0x3fbb0000274540e3),
            f64::from_bits(0x3fbb19be7ae68517),
            f64::from_bits(0x4022e4813637e946),
            f64::from_bits(0x3bc307f88dbd9d93),
        ],
        [
            f64::from_bits(0x3fbbfffffebf2e9b),
            f64::from_bits(0x3fbc1cb88369cd19),
            f64::from_bits(0x4022367617aef223),
            f64::from_bits(0x3bc50038424a9cf3),
        ],
        [
            f64::from_bits(0x3fbd000023529045),
            f64::from_bits(0x3fbd1febc11d7ef7),
            f64::from_bits(0x4021945fb8e43d4e),
            f64::from_bits(0x3b81200752a6f224),
        ],
        [
            f64::from_bits(0x3fbdffffd872a829),
            f64::from_bits(0x3fbe23598ee4d6b7),
            f64::from_bits(0x4020fd0c76195d5f),
            f64::from_bits(0xbbb4d9ab85fdca85),
        ],
        [
            f64::from_bits(0x3fbeffffff323b84),
            f64::from_bits(0x3fbf2704ec9073e5),
            f64::from_bits(0x40206f713020200f),
            f64::from_bits(0x3bb77aa212836992),
        ],
        [
            f64::from_bits(0x3fc000000ce79195),
            f64::from_bits(0x3fc01577bc30cc61),
            f64::from_bits(0x401fd549d6564a88),
            f64::from_bits(0xbbc8926f965c0ad0),
        ],
        [
            f64::from_bits(0x3fc07fffee40e918),
            f64::from_bits(0x3fc0978d8279ac01),
            f64::from_bits(0x401edbb59294bc03),
            f64::from_bits(0xbb80a5334aae45d6),
        ],
        [
            f64::from_bits(0x3fc100000cc091fd),
            f64::from_bits(0x3fc119c544dfb2f7),
            f64::from_bits(0x401df0bb067d8e18),
            f64::from_bits(0xbbcc2c184ff642a4),
        ],
        [
            f64::from_bits(0x3fc180000d9936a1),
            f64::from_bits(0x3fc19c1fb9085a4b),
            f64::from_bits(0x401d131a71ce3629),
            f64::from_bits(0xbbc365530669355b),
        ],
        [
            f64::from_bits(0x3fc1ffffed5f3188),
            f64::from_bits(0x3fc21e9dee74bf2d),
            f64::from_bits(0x401c41b6ff0cd655),
            f64::from_bits(0x3b8867f5478ecfc5),
        ],
        [
            f64::from_bits(0x3fc2800005f06a51),
            f64::from_bits(0x3fc2a141550b313f),
            f64::from_bits(0x401b7b921702e6d2),
            f64::from_bits(0xbbadab51380131fe),
        ],
        [
            f64::from_bits(0x3fc2fffffe3d339e),
            f64::from_bits(0x3fc3240aa75f76df),
            f64::from_bits(0x401abfc8fcb6409d),
            f64::from_bits(0x3bc60bcf0d291d83),
        ],
        [
            f64::from_bits(0x3fc37fffed888d6f),
            f64::from_bits(0x3fc3a6fb13cc5db7),
            f64::from_bits(0x401a0d8f8ed5320d),
            f64::from_bits(0x3bb8a48e4eef03ab),
        ],
        [
            f64::from_bits(0x3fc4000002ca050d),
            f64::from_bits(0x3fc42a13e25776bb),
            f64::from_bits(0x4019642dfa84c2bc),
            f64::from_bits(0xbbd0bd5dcc56516f),
        ],
        [
            f64::from_bits(0x3fc47ffff2531f5c),
            f64::from_bits(0x3fc4ad55deb73404),
            f64::from_bits(0x4018c2fef86e9035),
            f64::from_bits(0x3b9cffe75aa287c8),
        ],
        [
            f64::from_bits(0x3fc5000013774992),
            f64::from_bits(0x3fc530c27d0ee307),
            f64::from_bits(0x4018296c370caf35),
            f64::from_bits(0xbbcf75d1f91d6532),
        ],
        [
            f64::from_bits(0x3fc57fffedddcb2d),
            f64::from_bits(0x3fc5b45a5db4347d),
            f64::from_bits(0x401796ee52190c0e),
            f64::from_bits(0x3b88a25f17d5d076),
        ],
        [
            f64::from_bits(0x3fc5fffff41949a0),
            f64::from_bits(0x3fc6381f13bf986a),
            f64::from_bits(0x40170b092d2255fd),
            f64::from_bits(0xbb9bfb23b1bcd5e7),
        ],
        [
            f64::from_bits(0x3fc67ffff834d3a1),
            f64::from_bits(0x3fc6bc118ec85952),
            f64::from_bits(0x4016854c62cf2268),
            f64::from_bits(0x3b9ee53b82e39e04),
        ],
        [
            f64::from_bits(0x3fc6fffffd9106ea),
            f64::from_bits(0x3fc74032f298f6f7),
            f64::from_bits(0x401605511f4f84a9),
            f64::from_bits(0xbbb59c4a112634b8),
        ],
        [
            f64::from_bits(0x3fc780000f649a4f),
            f64::from_bits(0x3fc7c4846ca53abc),
            f64::from_bits(0x40158ab94809d175),
            f64::from_bits(0x3bc91c7573d3cd2e),
        ],
        [
            f64::from_bits(0x3fc7ffffef06bbd8),
            f64::from_bits(0x3fc84906df7d76ad),
            f64::from_bits(0x4015152edd2b30a6),
            f64::from_bits(0xbbbfa2da084c3eef),
        ],
        [
            f64::from_bits(0x3fc88000021c6334),
            f64::from_bits(0x3fc8cdbbd965f986),
            f64::from_bits(0x4014a46251b74296),
            f64::from_bits(0xbb9ec02e74dcfe0b),
        ],
        [
            f64::from_bits(0x3fc8fffff38d0756),
            f64::from_bits(0x3fc952a428e173c7),
            f64::from_bits(0x4014380b17b59ebd),
            f64::from_bits(0xbbcd0f1cb77589f0),
        ],
        [
            f64::from_bits(0x3fc98000104efca1),
            f64::from_bits(0x3fc9d7c14644d23c),
            f64::from_bits(0x4013cfe5cb1eabd5),
            f64::from_bits(0xbbd5d6f7ea188d9e),
        ],
        [
            f64::from_bits(0x3fca000009417b30),
            f64::from_bits(0x3fca5d14096d76aa),
            f64::from_bits(0x40136bb4b3723db0),
            f64::from_bits(0x3bbe3e0dfbf3979c),
        ],
        [
            f64::from_bits(0x3fca7fffeb1c23ec),
            f64::from_bits(0x3fcae29dab60288d),
            f64::from_bits(0x40130b3e783071d7),
            f64::from_bits(0xbbc7dd823d5384bf),
        ],
        [
            f64::from_bits(0x3fcafffffb171c13),
            f64::from_bits(0x3fcb685fa221a96b),
            f64::from_bits(0x4012ae4dd8c0747d),
            f64::from_bits(0x3bd4644bd5554972),
        ],
        [
            f64::from_bits(0x3fcb80000aba44be),
            f64::from_bits(0x3fcbee5aecdf241f),
            f64::from_bits(0x401254b1c6fad63b),
            f64::from_bits(0x3ba41916d092b85a),
        ],
        [
            f64::from_bits(0x3fcc0000113d2a3e),
            f64::from_bits(0x3fcc7490b3e92543),
            f64::from_bits(0x4011fe3c9a62c035),
            f64::from_bits(0xbba3cc3941a03739),
        ],
        [
            f64::from_bits(0x3fcc7ffff49e00ce),
            f64::from_bits(0x3fccfb020f59eab0),
            f64::from_bits(0x4011aac3e956a631),
            f64::from_bits(0xbbb7a383bfa8cb5b),
        ],
        [
            f64::from_bits(0x3fcd000005f611ab),
            f64::from_bits(0x3fcd81b089e6844e),
            f64::from_bits(0x40115a1ff391268d),
            f64::from_bits(0x3bd39b5cb2dc91f3),
        ],
        [
            f64::from_bits(0x3fcd800014764ceb),
            f64::from_bits(0x3fce089d27debf0d),
            f64::from_bits(0x40110c2bfbc84740),
            f64::from_bits(0x3bc14d4d84712510),
        ],
        [
            f64::from_bits(0x3fce000014bcea76),
            f64::from_bits(0x3fce8fc916dbc820),
            f64::from_bits(0x4010c0c5a00ca48e),
            f64::from_bits(0xbbd33788640f1b9e),
        ],
        [
            f64::from_bits(0x3fce7ffffd7995bd),
            f64::from_bits(0x3fcf173588b50424),
            f64::from_bits(0x401077ccbe02169a),
            f64::from_bits(0xbbb61fee221fdf77),
        ],
        [
            f64::from_bits(0x3fcf00000cc35436),
            f64::from_bits(0x3fcf9ee3fd21a40b),
            f64::from_bits(0x401031231ee7ffe8),
            f64::from_bits(0x3bd427e3c79ff5c1),
        ],
        [
            f64::from_bits(0x3fcf800001d1da33),
            f64::from_bits(0x3fd0136ab7dbe15c),
            f64::from_bits(0x400fd95977d559e5),
            f64::from_bits(0x3bb0c6a1d67948d7),
        ],
        [
            f64::from_bits(0x3fd00000060c13b2),
            f64::from_bits(0x3fd05785aaad4f18),
            f64::from_bits(0x400f549e2675d182),
            f64::from_bits(0xbbc1520818f0dd10),
        ],
        [
            f64::from_bits(0x3fd0400003885492),
            f64::from_bits(0x3fd09bc3660542d7),
            f64::from_bits(0x400ed3e2df3f5fec),
            f64::from_bits(0xbbd95657b883ae62),
        ],
        [
            f64::from_bits(0x3fd08000052f5a13),
            f64::from_bits(0x3fd0e0249a195045),
            f64::from_bits(0x400e56f8fa68f2c8),
            f64::from_bits(0x3bded7ba5a543e8e),
        ],
        [
            f64::from_bits(0x3fd0c00002ba1af5),
            f64::from_bits(0x3fd124a9e2e7f24b),
            f64::from_bits(0x400dddb4bffe633f),
            f64::from_bits(0xbbdcba860c60278f),
        ],
        [
            f64::from_bits(0x3fd0fffff76642c1),
            f64::from_bits(0x3fd16953e162ffe6),
            f64::from_bits(0x400d67ed0311d5d5),
            f64::from_bits(0x3b7b1f4ae40c5f9e),
        ],
        [
            f64::from_bits(0x3fd14000033602f0),
            f64::from_bits(0x3fd1ae235f49508e),
            f64::from_bits(0x400cf57ab8708266),
            f64::from_bits(0xbbd6a6c28620f301),
        ],
        [
            f64::from_bits(0x3fd17ffffefd1a13),
            f64::from_bits(0x3fd1f318db2a9ba1),
            f64::from_bits(0x400c86398d11009e),
            f64::from_bits(0x3bd3a9c669b21d3b),
        ],
        [
            f64::from_bits(0x3fd1bffff718365d),
            f64::from_bits(0x3fd238350c41e3ac),
            f64::from_bits(0x400c1a06e02be47c),
            f64::from_bits(0x3bdb961a129e8cd1),
        ],
        [
            f64::from_bits(0x3fd1ffffff001e00),
            f64::from_bits(0x3fd27d78b2f6395e),
            f64::from_bits(0x400bb0c1f2fe9a85),
            f64::from_bits(0x3be074a9e68fd7d8),
        ],
        [
            f64::from_bits(0x3fd23ffffe425a6a),
            f64::from_bits(0x3fd2c2e4618faabe),
            f64::from_bits(0x400b4a4c190b18df),
            f64::from_bits(0xbbdf0d1ff615aad1),
        ],
        [
            f64::from_bits(0x3fd28000059ec1db),
            f64::from_bits(0x3fd30878d8583884),
            f64::from_bits(0x400ae6880cd82bc2),
            f64::from_bits(0xbbd563c3141c1f8d),
        ],
        [
            f64::from_bits(0x3fd2c000000dd081),
            f64::from_bits(0x3fd34e36affdb6d8),
            f64::from_bits(0x400a855a5270fc15),
            f64::from_bits(0xbbc6d88d9f2cdafd),
        ],
        [
            f64::from_bits(0x3fd2fffffc1dcd2b),
            f64::from_bits(0x3fd3941ea95875bc),
            f64::from_bits(0x400a26a8aa9502b6),
            f64::from_bits(0xbbe13cad8389b15c),
        ],
        [
            f64::from_bits(0x3fd33ffff6c0d4a0),
            f64::from_bits(0x3fd3da31739845f5),
            f64::from_bits(0x4009ca5a4d2573a0),
            f64::from_bits(0xbbc71636acaee379),
        ],
        [
            f64::from_bits(0x3fd3800006b16793),
            f64::from_bits(0x3fd4206fdbc088f0),
            f64::from_bits(0x400970579344e33a),
            f64::from_bits(0xbbc2c0521d7a4f81),
        ],
        [
            f64::from_bits(0x3fd3c00007358fa3),
            f64::from_bits(0x3fd466da6f23311d),
            f64::from_bits(0x4009188a5aa612ea),
            f64::from_bits(0x3b8653a5685e8edc),
        ],
        [
            f64::from_bits(0x3fd3fffffc3b18cf),
            f64::from_bits(0x3fd4ad71e9282e6b),
            f64::from_bits(0x4008c2dd641e643d),
            f64::from_bits(0x3b95f0ef3f567c64),
        ],
        [
            f64::from_bits(0x3fd44000000dd2a8),
            f64::from_bits(0x3fd4f4371fa3f2d1),
            f64::from_bits(0x40086f3c6072f821),
            f64::from_bits(0x3bb68efa95ff68b5),
        ],
        [
            f64::from_bits(0x3fd47ffffbb43713),
            f64::from_bits(0x3fd53b2ab3ac333c),
            f64::from_bits(0x40081d943da56692),
            f64::from_bits(0xbbbf4d7f2985fd3f),
        ],
        [
            f64::from_bits(0x3fd4bffffb113bf4),
            f64::from_bits(0x3fd5824d6e8ed9c2),
            f64::from_bits(0x4007cdd2a8add00f),
            f64::from_bits(0x3bcf478a1c9b3657),
        ],
        [
            f64::from_bits(0x3fd4fffff7f087c9),
            f64::from_bits(0x3fd5c9a007446496),
            f64::from_bits(0x40077fe6444588eb),
            f64::from_bits(0xbbc177dca4eabb0c),
        ],
        [
            f64::from_bits(0x3fd54000088b3814),
            f64::from_bits(0x3fd61123564125f9),
            f64::from_bits(0x400733be6281a765),
            f64::from_bits(0xbbc2c52cf57051c4),
        ],
        [
            f64::from_bits(0x3fd57ffff7d55966),
            f64::from_bits(0x3fd658d7e194a5d5),
            f64::from_bits(0x4006e94b73b47d1f),
            f64::from_bits(0x3bda2fcff9996dc6),
        ],
        [
            f64::from_bits(0x3fd5c00008bf2490),
            f64::from_bits(0x3fd6a0beb775b28d),
            f64::from_bits(0x4006a07e15b6ec28),
            f64::from_bits(0xbbe0ca90aa5285b8),
        ],
        [
            f64::from_bits(0x3fd6000009fa853f),
            f64::from_bits(0x3fd6e8d865a66cfd),
            f64::from_bits(0x400659481c701269),
            f64::from_bits(0x3bd9ea958591e13a),
        ],
        [
            f64::from_bits(0x3fd6400007595fca),
            f64::from_bits(0x3fd73125c0556a7c),
            f64::from_bits(0x4006139bbaae9d02),
            f64::from_bits(0x3bd88aff40152b83),
        ],
        [
            f64::from_bits(0x3fd68000031687da),
            f64::from_bits(0x3fd779a792e2cfd0),
            f64::from_bits(0x4005cf6bcae0882b),
            f64::from_bits(0xbbd8a4a29f439451),
        ],
        [
            f64::from_bits(0x3fd6bffff5c8cfe2),
            f64::from_bits(0x3fd7c25e9fb452ed),
            f64::from_bits(0x40058cabc561f1cd),
            f64::from_bits(0xbbe371a6f6a37d74),
        ],
        [
            f64::from_bits(0x3fd6fffff81df231),
            f64::from_bits(0x3fd80b4bcfb4dab5),
            f64::from_bits(0x40054b4f8d3ca5d3),
            f64::from_bits(0x3bcb4686679dc99f),
        ],
        [
            f64::from_bits(0x3fd73ffffa71385e),
            f64::from_bits(0x3fd8546fe007a9b6),
            f64::from_bits(0x40050b4bb3b22176),
            f64::from_bits(0xbbcd1540a5c73477),
        ],
        [
            f64::from_bits(0x3fd78000024a9c2b),
            f64::from_bits(0x3fd89dcba7fcf5cf),
            f64::from_bits(0x4004cc953159cbe1),
            f64::from_bits(0xbbdc25ead58a6ad0),
        ],
        [
            f64::from_bits(0x3fd7c00002eb62b8),
            f64::from_bits(0x3fd8e75fec0ba5cf),
            f64::from_bits(0x40048f218731eeea),
            f64::from_bits(0xbbc1cb73cc1adafb),
        ],
        [
            f64::from_bits(0x3fd80000054a52d1),
            f64::from_bits(0x3fd9312d8bb822e9),
            f64::from_bits(0x400452e69170a729),
            f64::from_bits(0xbbd8bb17eac002ee),
        ],
        [
            f64::from_bits(0x3fd83ffff93a00a3),
            f64::from_bits(0x3fd97b354bb9ad2a),
            f64::from_bits(0x400417daae924e7f),
            f64::from_bits(0x3bd4b8009a378cc7),
        ],
        [
            f64::from_bits(0x3fd87ffffbdc91c1),
            f64::from_bits(0x3fd9c5782771b601),
            f64::from_bits(0x4003ddf478855799),
            f64::from_bits(0x3bd9077da00445d9),
        ],
        [
            f64::from_bits(0x3fd8bffff6d215e6),
            f64::from_bits(0x3fda0ff6e0ea4a0b),
            f64::from_bits(0x4003a52b189a0989),
            f64::from_bits(0xbbda683189c0613d),
        ],
        [
            f64::from_bits(0x3fd9000002f734ef),
            f64::from_bits(0x3fda5ab2736bf579),
            f64::from_bits(0x40036d75e9244ca6),
            f64::from_bits(0x3be3a6d84b722377),
        ],
        [
            f64::from_bits(0x3fd9400004eef8b4),
            f64::from_bits(0x3fdaa5ab9fb6e3d0),
            f64::from_bits(0x400336ccc9089cb7),
            f64::from_bits(0x3b9f696322cc00bb),
        ],
        [
            f64::from_bits(0x3fd98000041ec76a),
            f64::from_bits(0x3fdaf0e35176c7e4),
            f64::from_bits(0x40030127cb0b9506),
            f64::from_bits(0x3bb1ffdb5385a849),
        ],
        [
            f64::from_bits(0x3fd9c00008044e47),
            f64::from_bits(0x3fdb3c5a77071224),
            f64::from_bits(0x4002cc7f50d75ec7),
            f64::from_bits(0xbbb0fade78effc8a),
        ],
        [
            f64::from_bits(0x3fda000001f8235b),
            f64::from_bits(0x3fdb8811e725782e),
            f64::from_bits(0x400298cc18fbfb37),
            f64::from_bits(0xbbe55ed33b50e71b),
        ],
        [
            f64::from_bits(0x3fda3ffffb8c6f08),
            f64::from_bits(0x3fdbd40a97b086f3),
            f64::from_bits(0x40026607154de04b),
            f64::from_bits(0xbbdec65e455faae3),
        ],
        [
            f64::from_bits(0x3fda7ffffb3d63e1),
            f64::from_bits(0x3fdc20457d9a3b8a),
            f64::from_bits(0x400234297e60bfbb),
            f64::from_bits(0x3be3001c154ebd33),
        ],
        [
            f64::from_bits(0x3fdabffff5f45c48),
            f64::from_bits(0x3fdc6cc37b8d45e6),
            f64::from_bits(0x4002032cdb1ace69),
            f64::from_bits(0xbbe5ebf83ed33616),
        ],
        [
            f64::from_bits(0x3fdb00000508b34c),
            f64::from_bits(0x3fdcb985a27e8d37),
            f64::from_bits(0x4001d30ad4459a2b),
            f64::from_bits(0xbbd01432ae61e2d1),
        ],
        [
            f64::from_bits(0x3fdb40000a84710c),
            f64::from_bits(0x3fdd068cc3e50155),
            f64::from_bits(0x4001a3bd775034dd),
            f64::from_bits(0xbbe80b1e58e0e228),
        ],
        [
            f64::from_bits(0x3fdb7ffff692e9d8),
            f64::from_bits(0x3fdd53d9c49d6627),
            f64::from_bits(0x4001753efe18066a),
            f64::from_bits(0xbbb004c8f760d33e),
        ],
        [
            f64::from_bits(0x3fdbc0000280f14d),
            f64::from_bits(0x3fdda16de4e81013),
            f64::from_bits(0x40014789a38ea052),
            f64::from_bits(0x3be848bc27c9c4ea),
        ],
        [
            f64::from_bits(0x3fdc0000001121d1),
            f64::from_bits(0x3fddef49eac018f0),
            f64::from_bits(0x40011a9820b8be0c),
            f64::from_bits(0xbbe1527ed0d6010e),
        ],
        [
            f64::from_bits(0x3fdc3ffffef662aa),
            f64::from_bits(0x3fde3d6eea0c7070),
            f64::from_bits(0x4000ee6532f46ccd),
            f64::from_bits(0x3be8d241189a000d),
        ],
        [
            f64::from_bits(0x3fdc800009845818),
            f64::from_bits(0x3fde8bddf36a8b1b),
            f64::from_bits(0x4000c2ebcac73476),
            f64::from_bits(0x3bd221f712bed284),
        ],
        [
            f64::from_bits(0x3fdcbffffb0493bf),
            f64::from_bits(0x3fdeda97e0c60d10),
            f64::from_bits(0x40009827251c7836),
            f64::from_bits(0xbbe0bd546eec41b7),
        ],
        [
            f64::from_bits(0x3fdcfffffd52961f),
            f64::from_bits(0x3fdf299defb3e44b),
            f64::from_bits(0x40006e1274e459f5),
            f64::from_bits(0xbbd93f77e969c82f),
        ],
        [
            f64::from_bits(0x3fdd3ffffe2319a4),
            f64::from_bits(0x3fdf78f117139490),
            f64::from_bits(0x400044a93e737e94),
            f64::from_bits(0xbb91e7cc49594b7a),
        ],
        [
            f64::from_bits(0x3fdd7ffffa4de596),
            f64::from_bits(0x3fdfc892638f49e8),
            f64::from_bits(0x40001be7231057a5),
            f64::from_bits(0x3bd482b0f5af9f5f),
        ],
        [
            f64::from_bits(0x3fddbffffe729a69),
            f64::from_bits(0x3fe00c417c6ab019),
            f64::from_bits(0x3fffe78fbf612660),
            f64::from_bits(0x3bea5cda00da681e),
        ],
        [
            f64::from_bits(0x3fde000009d66802),
            f64::from_bits(0x3fe03461f6b883cf),
            f64::from_bits(0x3fff988ebc05a87c),
            f64::from_bits(0xbbe06c33f2372669),
        ],
        [
            f64::from_bits(0x3fde3ffffb211657),
            f64::from_bits(0x3fe05cab191db8e8),
            f64::from_bits(0x3fff4ac37bcfe6be),
            f64::from_bits(0xbbd5d51f5ed8d35b),
        ],
        [
            f64::from_bits(0x3fde80000a3f068a),
            f64::from_bits(0x3fe0851d95fb54f0),
            f64::from_bits(0x3ffefe26144ca408),
            f64::from_bits(0xbbc7c894a2c169c5),
        ],
        [
            f64::from_bits(0x3fdec00001adb060),
            f64::from_bits(0x3fe0adb9dc7b54f9),
            f64::from_bits(0x3ffeb2af5ebe52a7),
            f64::from_bits(0x3bd4e740312c5ffd),
        ],
        [
            f64::from_bits(0x3fdeffffff5c0d01),
            f64::from_bits(0x3fe0d68092550a8d),
            f64::from_bits(0x3ffe68580d71fdf0),
            f64::from_bits(0x3bddd8a696b35499),
        ],
        [
            f64::from_bits(0x3fdf3ffff93d5fcc),
            f64::from_bits(0x3fe0ff7245cb4374),
            f64::from_bits(0x3ffe1f193cce5040),
            f64::from_bits(0xbbc9f0ec7c1efab4),
        ],
        [
            f64::from_bits(0x3fdf7ffffa0dd18f),
            f64::from_bits(0x3fe1288f944dd508),
            f64::from_bits(0x3ffdd6ec298b874d),
            f64::from_bits(0x3bea6ebd9642a0a6),
        ],
        [
            f64::from_bits(0x3fdfbffffd3a9f1a),
            f64::from_bits(0x3fe151d913750f3e),
            f64::from_bits(0x3ffd8fca5806a27e),
            f64::from_bits(0x3bda2a03fc65ac7a),
        ],
        [
            f64::from_bits(0x3fdffffffc481400),
            f64::from_bits(0x3fe17b4f598944ca),
            f64::from_bits(0x3ffd49ad82532170),
            f64::from_bits(0x3bc4412e3d236dc3),
        ],
        [
            f64::from_bits(0x3fe01fffff53786c),
            f64::from_bits(0x3fe1a4f307d83d47),
            f64::from_bits(0x3ffd048f851bffeb),
            f64::from_bits(0x3bd1589d29f81b14),
        ],
        [
            f64::from_bits(0x3fe03ffffee301b7),
            f64::from_bits(0x3fe1cec4b8a6a382),
            f64::from_bits(0x3ffcc06a7c519db6),
            f64::from_bits(0x3bd370e65b24d6b2),
        ],
        [
            f64::from_bits(0x3fe06000006e36bf),
            f64::from_bits(0x3fe1f8c5114eb8be),
            f64::from_bits(0x3ffc7d38a34d6786),
            f64::from_bits(0xbbea92de4b98c1d4),
        ],
        [
            f64::from_bits(0x3fe07ffffd60aa43),
            f64::from_bits(0x3fe222f4abeccecb),
            f64::from_bits(0x3ffc3af477342ac4),
            f64::from_bits(0xbbdd47f603a5c2c2),
        ],
        [
            f64::from_bits(0x3fe0a000037762e8),
            f64::from_bits(0x3fe24d543f99efe8),
            f64::from_bits(0x3ffbf99875f54fab),
            f64::from_bits(0x3bedf7f415771a46),
        ],
        [
            f64::from_bits(0x3fe0bfffff1c6921),
            f64::from_bits(0x3fe277e4598e35d0),
            f64::from_bits(0x3ffbb91f8addd186),
            f64::from_bits(0x3be0f16c5e0e5a73),
        ],
        [
            f64::from_bits(0x3fe0dfffff07154b),
            f64::from_bits(0x3fe2a2a5b6bc3986),
            f64::from_bits(0x3ffb79848301646d),
            f64::from_bits(0xbbf02dd0bbaa5310),
        ],
        [
            f64::from_bits(0x3fe1000002fcdda4),
            f64::from_bits(0x3fe2cd9902a59f1e),
            f64::from_bits(0x3ffb3ac2705219bf),
            f64::from_bits(0xbbe59357112fa616),
        ],
        [
            f64::from_bits(0x3fe1200001ce1140),
            f64::from_bits(0x3fe2f8bedf0a67c2),
            f64::from_bits(0x3ffafcd49ab8ae2a),
            f64::from_bits(0x3be2c5429303f346),
        ],
        [
            f64::from_bits(0x3fe1400004d0f355),
            f64::from_bits(0x3fe3241808fcc7bf),
            f64::from_bits(0x3ffabfb6497b9a36),
            f64::from_bits(0x3bebc044b5a59234),
        ],
        [
            f64::from_bits(0x3fe1600000fb0c8a),
            f64::from_bits(0x3fe34fa52471618b),
            f64::from_bits(0x3ffa83630d26d117),
            f64::from_bits(0xbbdbfbb23f7bb7c9),
        ],
        [
            f64::from_bits(0x3fe18000026f10b3),
            f64::from_bits(0x3fe37b66f7579056),
            f64::from_bits(0x3ffa47d66b4cf4b1),
            f64::from_bits(0x3bf0f6b4af0b5de9),
        ],
        [
            f64::from_bits(0x3fe19ffffd0978f8),
            f64::from_bits(0x3fe3a75e290cc78c),
            f64::from_bits(0x3ffa0d0c36c21315),
            f64::from_bits(0x3beb2129a296b262),
        ],
        [
            f64::from_bits(0x3fe1bffffd94840b),
            f64::from_bits(0x3fe3d38b85b4e4a4),
            f64::from_bits(0x3ff9d30032f2ecef),
            f64::from_bits(0xbbdbab1ab9bb7d74),
        ],
        [
            f64::from_bits(0x3fe1dffffbda1ea1),
            f64::from_bits(0x3fe3ffefbf3cee2f),
            f64::from_bits(0x3ff999ae6770fed8),
            f64::from_bits(0x3bda0bdcb4ace9a4),
        ],
        [
            f64::from_bits(0x3fe1fffffc989533),
            f64::from_bits(0x3fe42c8b9c27900c),
            f64::from_bits(0x3ff96112e0d9f1ac),
            f64::from_bits(0xbbee19eb2fa2d81a),
        ],
        [
            f64::from_bits(0x3fe22000012b8d26),
            f64::from_bits(0x3fe4595fe11975ca),
            f64::from_bits(0x3ff92929cdaa4e80),
            f64::from_bits(0x3bf23382acc82d4b),
        ],
        [
            f64::from_bits(0x3fe2400004f4d6af),
            f64::from_bits(0x3fe4866d4d224131),
            f64::from_bits(0x3ff8f1ef815c34e8),
            f64::from_bits(0xbbd0c6ff3b740a99),
        ],
        [
            f64::from_bits(0x3fe25ffffcc07bda),
            f64::from_bits(0x3fe4b3b498b7d010),
            f64::from_bits(0x3ff8bb6073e7ffa1),
            f64::from_bits(0x3bebc31b1ad7a9c2),
        ],
        [
            f64::from_bits(0x3fe28000042d9639),
            f64::from_bits(0x3fe4e136b64540d1),
            f64::from_bits(0x3ff88578f4374938),
            f64::from_bits(0x3be36de91b85e901),
        ],
        [
            f64::from_bits(0x3fe2a00003be29a0),
            f64::from_bits(0x3fe50ef452bffd96),
            f64::from_bits(0x3ff85035c0042c06),
            f64::from_bits(0x3be15d0176f5efbd),
        ],
        [
            f64::from_bits(0x3fe2bffffaa91f12),
            f64::from_bits(0x3fe53cee3e2f4e0d),
            f64::from_bits(0x3ff81b938542df07),
            f64::from_bits(0x3be555cd17662a2b),
        ],
        [
            f64::from_bits(0x3fe2dffffe884891),
            f64::from_bits(0x3fe56b256c1a2470),
            f64::from_bits(0x3ff7e78ee422ea70),
            f64::from_bits(0x3bf03504bd030c11),
        ],
        [
            f64::from_bits(0x3fe2fffffe87152b),
            f64::from_bits(0x3fe5999a9beaaaa1),
            f64::from_bits(0x3ff7b424d18fe9b3),
            f64::from_bits(0xbb649a5f773e0e64),
        ],
        [
            f64::from_bits(0x3fe31fffffc1a721),
            f64::from_bits(0x3fe5c84eafe0e564),
            f64::from_bits(0x3ff78152338db8d4),
            f64::from_bits(0x3beaf4285da8e935),
        ],
        [
            f64::from_bits(0x3fe33fffff70a372),
            f64::from_bits(0x3fe5f74282191d64),
            f64::from_bits(0x3ff74f141122bcae),
            f64::from_bits(0x3bdb1c4bdee4bfaf),
        ],
        [
            f64::from_bits(0x3fe360000436e836),
            f64::from_bits(0x3fe62676fde6ccff),
            f64::from_bits(0x3ff71d677644252c),
            f64::from_bits(0xbbec3d10e08c3afb),
        ],
        [
            f64::from_bits(0x3fe37ffffcbe9641),
            f64::from_bits(0x3fe655ecee9ffdaf),
            f64::from_bits(0x3ff6ec49a6fc0515),
            f64::from_bits(0x3bdda4532ed29567),
        ],
        [
            f64::from_bits(0x3fe39fffffb6d6ca),
            f64::from_bits(0x3fe685a55e67a1e1),
            f64::from_bits(0x3ff6bbb7bc2ae969),
            f64::from_bits(0x3becbf7b2ef43882),
        ],
        [
            f64::from_bits(0x3fe3c00004934fec),
            f64::from_bits(0x3fe6b5a12cc07d75),
            f64::from_bits(0x3ff68baf10b02ef8),
            f64::from_bits(0xbbe7c8fbfeb7cabd),
        ],
        [
            f64::from_bits(0x3fe3e00003f5cf7f),
            f64::from_bits(0x3fe6e5e13e59def6),
            f64::from_bits(0x3ff65c2d0e61500f),
            f64::from_bits(0xbbe30ba4035f7845),
        ],
        [
            f64::from_bits(0x3fe4000005280ad9),
            f64::from_bits(0x3fe7166691ab4c3e),
            f64::from_bits(0x3ff62d2f19f01c90),
            f64::from_bits(0xbbf1e9f5ffe95f6a),
        ],
        [
            f64::from_bits(0x3fe42000049efb65),
            f64::from_bits(0x3fe7473218af3b9d),
            f64::from_bits(0x3ff5feb2b86465e4),
            f64::from_bits(0x3bc4cad7280d591e),
        ],
        [
            f64::from_bits(0x3fe440000035ccb6),
            f64::from_bits(0x3fe77844cb4ff1e5),
            f64::from_bits(0x3ff5d0b57c455428),
            f64::from_bits(0x3bed8c187ba5617c),
        ],
        [
            f64::from_bits(0x3fe4600003346717),
            f64::from_bits(0x3fe7a99fba258778),
            f64::from_bits(0x3ff5a334f4392254),
            f64::from_bits(0xbbefd14afc84a570),
        ],
        [
            f64::from_bits(0x3fe4800003002575),
            f64::from_bits(0x3fe7db43d836768f),
            f64::from_bits(0x3ff5762edcf97e0c),
            f64::from_bits(0xbbdd7eba5f5df49e),
        ],
        [
            f64::from_bits(0x3fe4a000055bf381),
            f64::from_bits(0x3fe80d3235edeefa),
            f64::from_bits(0x3ff549a0ea46e31f),
            f64::from_bits(0xbbdba52276823eac),
        ],
        [
            f64::from_bits(0x3fe4c00004ce10e3),
            f64::from_bits(0x3fe83f6bd67dc1a8),
            f64::from_bits(0x3ff51d88ed82bcc4),
            f64::from_bits(0xbbeae92d077d29ea),
        ],
        [
            f64::from_bits(0x3fe4e000016c60e1),
            f64::from_bits(0x3fe871f1ca0aaf31),
            f64::from_bits(0x3ff4f1e4bdacbf16),
            f64::from_bits(0x3be8295846ee425e),
        ],
        [
            f64::from_bits(0x3fe4ffffff966f0a),
            f64::from_bits(0x3fe8a4c52bff2dae),
            f64::from_bits(0x3ff4c6b23917657e),
            f64::from_bits(0xbbf127c25c86c705),
        ],
        [
            f64::from_bits(0x3fe520000076e6eb),
            f64::from_bits(0x3fe8d7e7175651e8),
            f64::from_bits(0x3ff49bef4f459b05),
            f64::from_bits(0xbbb1e9d14181bbfc),
        ],
        [
            f64::from_bits(0x3fe5400003d12d3b),
            f64::from_bits(0x3fe90b58a976ed56),
            f64::from_bits(0x3ff47199fdf24af4),
            f64::from_bits(0x3be38c17c30decaf),
        ],
        [
            f64::from_bits(0x3fe55ffffce7fa8d),
            f64::from_bits(0x3fe93f1af03a3a09),
            f64::from_bits(0x3ff447b05f13234b),
            f64::from_bits(0x3bf1b8b270df7e20),
        ],
        [
            f64::from_bits(0x3fe580000331b46a),
            f64::from_bits(0x3fe9732f38e83134),
            f64::from_bits(0x3ff41e3068d8b41b),
            f64::from_bits(0xbbee24d8b90bc28b),
        ],
        [
            f64::from_bits(0x3fe59ffffc14848e),
            f64::from_bits(0x3fe9a7968471b489),
            f64::from_bits(0x3ff3f5185de3aa73),
            f64::from_bits(0xbbecacd9e0761536),
        ],
        [
            f64::from_bits(0x3fe5bffffb7cd395),
            f64::from_bits(0x3fe9dc5224a8b955),
            f64::from_bits(0x3ff3cc664f8fff15),
            f64::from_bits(0xbbf67c9782045611),
        ],
        [
            f64::from_bits(0x3fe5e000000dcc40),
            f64::from_bits(0x3fea11634df5b93e),
            f64::from_bits(0x3ff3a41875853228),
            f64::from_bits(0xbbf585dad481f350),
        ],
        [
            f64::from_bits(0x3fe6000002efd2fc),
            f64::from_bits(0x3fea46cb30d16323),
            f64::from_bits(0x3ff37c2d187962ae),
            f64::from_bits(0x3bf004c3a5f77bb0),
        ],
        [
            f64::from_bits(0x3fe61ffffeb8088a),
            f64::from_bits(0x3fea7c8b053920c0),
            f64::from_bits(0x3ff354a2891769a9),
            f64::from_bits(0x3bbc6b303fee3029),
        ],
        [
            f64::from_bits(0x3fe6400000f3ca06),
            f64::from_bits(0x3feab2a428a1911a),
            f64::from_bits(0x3ff32d770a6f0a4a),
            f64::from_bits(0x3bf2a6f8fac5081a),
        ],
        [
            f64::from_bits(0x3fe65ffffe9ec2f4),
            f64::from_bits(0x3feae917d4ce7239),
            f64::from_bits(0x3ff306a90751a948),
            f64::from_bits(0xbbe950b551ab9dbd),
        ],
        [
            f64::from_bits(0x3fe6800003d43966),
            f64::from_bits(0x3feb1fe7708b998a),
            f64::from_bits(0x3ff2e036d7a153c7),
            f64::from_bits(0x3bdd36e2a1e4a14e),
        ],
        [
            f64::from_bits(0x3fe69ffffab67783),
            f64::from_bits(0x3feb57142e575464),
            f64::from_bits(0x3ff2ba1f05006cb6),
            f64::from_bits(0x3bea9a4a473c2e31),
        ],
        [
            f64::from_bits(0x3fe6bffffcb65f89),
            f64::from_bits(0x3feb8e9f981efd2f),
            f64::from_bits(0x3ff2945fe948d9f7),
            f64::from_bits(0xbbca5294e802df72),
        ],
        [
            f64::from_bits(0x3fe6dffffc5609a9),
            f64::from_bits(0x3febc68afaed6ff1),
            f64::from_bits(0x3ff26ef81533411e),
            f64::from_bits(0xbbf89153f51bc566),
        ],
        [
            f64::from_bits(0x3fe6fffffc4eef86),
            f64::from_bits(0x3febfed7c62205fe),
            f64::from_bits(0x3ff249e60e70978c),
            f64::from_bits(0x3bc39021a2b9ff56),
        ],
        [
            f64::from_bits(0x3fe72000004d98b3),
            f64::from_bits(0x3fec3787716968ad),
            f64::from_bits(0x3ff2252861be7751),
            f64::from_bits(0x3befc9c574ee2211),
        ],
        [
            f64::from_bits(0x3fe73ffffc155075),
            f64::from_bits(0x3fec709b5ec6fd4e),
            f64::from_bits(0x3ff200bdb5d53311),
            f64::from_bits(0x3be28a4da269ae63),
        ],
        [
            f64::from_bits(0x3fe760000498c203),
            f64::from_bits(0x3fecaa15323d08c1),
            f64::from_bits(0x3ff1dca493433f65),
            f64::from_bits(0x3bf8cae414a28fb7),
        ],
        [
            f64::from_bits(0x3fe77fffff1e5636),
            f64::from_bits(0x3fece3f64147c12c),
            f64::from_bits(0x3ff1b8dbbfe294a8),
            f64::from_bits(0xbbe7e19c4b56a744),
        ],
        [
            f64::from_bits(0x3fe7a0000226d45a),
            f64::from_bits(0x3fed1e404120eb7f),
            f64::from_bits(0x3ff19561d15f8278),
            f64::from_bits(0x3be64b28032c5d4c),
        ],
        [
            f64::from_bits(0x3fe7c0000250a5aa),
            f64::from_bits(0x3fed58f4b112a1e1),
            f64::from_bits(0x3ff172358a59d565),
            f64::from_bits(0xbbe716deb8dc7867),
        ],
        [
            f64::from_bits(0x3fe7e0000482f82e),
            f64::from_bits(0x3fed94153576bdf0),
            f64::from_bits(0x3ff14f55a22a1c5b),
            f64::from_bits(0x3bf207e1e1305604),
        ],
        [
            f64::from_bits(0x3fe800000205003e),
            f64::from_bits(0x3fedcfa364d69ff7),
            f64::from_bits(0x3ff12cc0e37eb26f),
            f64::from_bits(0xbbd52ec6e32395f8),
        ],
        [
            f64::from_bits(0x3fe81ffffbf99411),
            f64::from_bits(0x3fee0ba0ebf98f51),
            f64::from_bits(0x3ff10a7616ddd5d6),
            f64::from_bits(0xbbece0d659866045),
        ],
        [
            f64::from_bits(0x3fe840000248e3a3),
            f64::from_bits(0x3fee480f9bb7f565),
            f64::from_bits(0x3ff0e873fb84e05c),
            f64::from_bits(0x3bf4e5e81595df92),
        ],
        [
            f64::from_bits(0x3fe860000145c157),
            f64::from_bits(0x3fee84f10a10b3ab),
            f64::from_bits(0x3ff0c6b97cbd7b1e),
            f64::from_bits(0xbbe19de6d5f121d0),
        ],
        [
            f64::from_bits(0x3fe88000022631b9),
            f64::from_bits(0x3feec2470be1f047),
            f64::from_bits(0x3ff0a5456d0b3ee6),
            f64::from_bits(0xbbc272b1a3ba2c6f),
        ],
        [
            f64::from_bits(0x3fe8a000045f7828),
            f64::from_bits(0x3fef00136c45ba1c),
            f64::from_bits(0x3ff08416af2a0f09),
            f64::from_bits(0x3be82b565b63c799),
        ],
        [
            f64::from_bits(0x3fe8bfffffc686cf),
            f64::from_bits(0x3fef3e57f03c824b),
            f64::from_bits(0x3ff0632c33502220),
            f64::from_bits(0xbbd039ad2dbeeb25),
        ],
        [
            f64::from_bits(0x3fe8dffffd8644c6),
            f64::from_bits(0x3fef7d168774261d),
            f64::from_bits(0x3ff04284dd5b3019),
            f64::from_bits(0x3bd79f33e1eba933),
        ],
        [
            f64::from_bits(0x3fe8fffffe4e7937),
            f64::from_bits(0x3fefbc511a99a641),
            f64::from_bits(0x3ff0221f9f69840b),
            f64::from_bits(0xbbea9e847beee018),
        ],
        [
            f64::from_bits(0x3fe920000435251f),
            f64::from_bits(0x3feffc099eb22390),
            f64::from_bits(0x3ff001fb6f7c51e8),
            f64::from_bits(0xbb5a12e731032e0a),
        ],
    ];
    let mut ux: i32 = 0;
    let mut i: i32 = 0;
    let mut n: i32 = 0;
    let mut a: f64 = 0.0;
    let mut da: f64 = 0.0;
    let mut a2: f64 = 0.0;
    let mut b: f64 = 0.0;
    let mut db: f64 = 0.0;
    let mut c: f64 = 0.0;
    let mut dc: f64 = 0.0;
    let mut fi: f64 = 0.0;
    let mut gi: f64 = 0.0;
    let mut pz: f64 = 0.0;
    let mut s: f64 = 0.0;
    let mut sy: f64 = 0.0;
    let mut t: f64 = 0.0;
    let mut t1: f64 = 0.0;
    let mut t2: f64 = 0.0;
    let mut t3: f64 = 0.0;
    let mut t4: f64 = 0.0;
    let mut w: f64 = 0.0;
    let mut x2: f64 = 0.0;
    let mut xn: f64 = 0.0;
    let mut y: f64 = 0.0;
    let mut ya: f64 = 0.0;
    let mut yya: f64 = 0.0;
    let mut z0: f64 = 0.0;
    let mut z: f64 = 0.0;
    let mut z2: f64 = 0.0;
    let mut num: f64 = 0.0;
    let mut v: f64 = 0.0;
    let mut retval: f64 = 0.0;
    num = x;
    ux = hi(num);
    if ((ux & 2146435072_i32) == 2146435072_i32) {
        retval = f64::from_bits(0xfff8000000000000);
        return retval;
    }
    w = (if (x < 0.0_f64) { (-x) } else { x });
    if (w <= g1) {
        retval = x;
        return retval;
    }
    if (w <= g2) {
        x2 = (x * x);
        t2 = (x2).mul_add(d11, d9);
        t2 = (x2).mul_add(t2, d7);
        t2 = (x2).mul_add(t2, d5);
        t2 = (x2).mul_add(t2, d3);
        y = (t2).mul_add((x * x2), x);
        retval = y;
        return retval;
    }
    if (w <= g3) {
        i = (256_f64.mul_add(w, mfftnhf) as i32);
        z = (w - xfg[(i) as usize][(0_i32) as usize]);
        z2 = (z * z);
        s = ((if (x < 0.0_f64) { (-1_i32) } else { 1_i32 }) as f64);
        pz = (z * z2).mul_add((z2).mul_add(e1, e0), z);
        fi = xfg[(i) as usize][(1_i32) as usize];
        gi = xfg[(i) as usize][(2_i32) as usize];
        t2 = ((pz * (gi + fi)) / (gi - pz));
        y = (fi + t2);
        retval = (s * y);
        return retval;
    }
    if (w <= g4) {
        t = (x).mul_add(hpinv, (toint));
        xn = (t - toint);
        v = t;
        t1 = (-(xn)).mul_add(mp2, (-(xn)).mul_add(mp1, x));
        n = (lo(v) & 1_i32);
        da = (xn * mp3);
        a = (-xn).mul_add(mp3, t1);
        da = (-xn).mul_add(mp3, t1 - a);
        if (a < 0.0_f64) {
            ya = (-a);
            yya = (-da);
            sy = ((-1_i32) as f64);
        } else {
            ya = a;
            yya = da;
            sy = 1_f64;
        }
        if (ya <= gy2) {
            a2 = (a * a);
            t2 = (a2).mul_add(d11, d9);
            t2 = (a2).mul_add(t2, d7);
            t2 = (a2).mul_add(t2, d5);
            t2 = (a2).mul_add(t2, d3);
            t2 = (a * a2).mul_add(t2, da);
            if (n != 0) {
                b = (a + t2);
                db = (if ((a).abs() > (t2).abs()) {
                    ((a - b) + t2)
                } else {
                    ((t2 - b) + a)
                });
                t1 = (1.0_f64 / b);
                t3 = (t1 * b);
                t4 = (t1).mul_add(b, (-t3));
                t2 = ((-(t1)).mul_add(db, (((1.0_f64 - t3) - t4) + 0.0_f64)) / b);
                c = (t1 + t2);
                dc = ((t1 - c) + t2);
                y = (c + dc);
                retval = (-y);
                return retval;
            } else {
                y = (a + t2);
                retval = y;
                return retval;
            }
        }
        i = (256_f64.mul_add(ya, mfftnhf) as i32);
        z = ({
            z0 = (ya - xfg[(i) as usize][(0_i32) as usize]);
            z0
        } + yya);
        z2 = (z * z);
        pz = (z * z2).mul_add((z2).mul_add(e1, e0), z);
        fi = xfg[(i) as usize][(1_i32) as usize];
        gi = xfg[(i) as usize][(2_i32) as usize];
        if (n != 0) {
            t2 = ((pz * (fi + gi)) / (fi + pz));
            y = (gi - t2);
            retval = ((-sy) * y);
            return retval;
        } else {
            t2 = ((pz * (gi + fi)) / (gi - pz));
            y = (fi + t2);
            retval = (sy * y);
            return retval;
        }
    }
    if (w <= g5) {
        t = (x).mul_add(hpinv, (toint));
        xn = (t - toint);
        v = t;
        t1 = (-(xn)).mul_add(mp2, (-(xn)).mul_add(mp1, x));
        n = (lo(v) & 1_i32);
        da = (xn * pp3);
        t = (-xn).mul_add(pp3, t1);
        da = (-xn).mul_add(pp3, t1 - t);
        t1 = (xn * pp4);
        a = (-xn).mul_add(pp4, t);
        da = (-xn).mul_add(pp4, t - a) + da;
        t1 = (a + da);
        t2 = (if ((a).abs() > (da).abs()) {
            ((a - t1) + da)
        } else {
            ((da - t1) + a)
        });
        a = t1;
        da = t2;
        if (a < 0.0_f64) {
            ya = (-a);
            yya = (-da);
            sy = ((-1_i32) as f64);
        } else {
            ya = a;
            yya = da;
            sy = 1_f64;
        }
        if (ya <= gy2) {
            a2 = (a * a);
            t2 = (a2).mul_add(d11, d9);
            t2 = (a2).mul_add(t2, d7);
            t2 = (a2).mul_add(t2, d5);
            t2 = (a2).mul_add(t2, d3);
            t2 = (a * a2).mul_add(t2, da);
            if (n != 0) {
                b = (a + t2);
                db = (if ((a).abs() > (t2).abs()) {
                    ((a - b) + t2)
                } else {
                    ((t2 - b) + a)
                });
                t1 = (1.0_f64 / b);
                t3 = (t1 * b);
                t4 = (t1).mul_add(b, (-t3));
                t2 = ((-(t1)).mul_add(db, (((1.0_f64 - t3) - t4) + 0.0_f64)) / b);
                c = (t1 + t2);
                dc = ((t1 - c) + t2);
                y = (c + dc);
                retval = (-y);
                return retval;
            } else {
                y = (a + t2);
                retval = y;
                return retval;
            }
        }
        i = (256_f64.mul_add(ya, mfftnhf) as i32);
        z = ({
            z0 = (ya - xfg[(i) as usize][(0_i32) as usize]);
            z0
        } + yya);
        z2 = (z * z);
        pz = (z * z2).mul_add((z2).mul_add(e1, e0), z);
        fi = xfg[(i) as usize][(1_i32) as usize];
        gi = xfg[(i) as usize][(2_i32) as usize];
        if (n != 0) {
            t2 = ((pz * (fi + gi)) / (fi + pz));
            y = (gi - t2);
            retval = ((-sy) * y);
            return retval;
        } else {
            t2 = ((pz * (gi + fi)) / (gi - pz));
            y = (fi + t2);
            retval = (sy * y);
            return retval;
        }
    }
    n = (branred(x, &mut a, &mut da) & 1_i32);
    t1 = (a + da);
    t2 = (if ((a).abs() > (da).abs()) {
        ((a - t1) + da)
    } else {
        ((da - t1) + a)
    });
    a = t1;
    da = t2;
    if (a < 0.0_f64) {
        ya = (-a);
        yya = (-da);
        sy = ((-1_i32) as f64);
    } else {
        ya = a;
        yya = da;
        sy = 1_f64;
    }
    if (ya <= gy2) {
        a2 = (a * a);
        t2 = (a2).mul_add(d11, d9);
        t2 = (a2).mul_add(t2, d7);
        t2 = (a2).mul_add(t2, d5);
        t2 = (a2).mul_add(t2, d3);
        t2 = (a * a2).mul_add(t2, da);
        if (n != 0) {
            b = (a + t2);
            db = (if ((a).abs() > (t2).abs()) {
                ((a - b) + t2)
            } else {
                ((t2 - b) + a)
            });
            t1 = (1.0_f64 / b);
            t3 = (t1 * b);
            t4 = (t1).mul_add(b, (-t3));
            t2 = ((-(t1)).mul_add(db, (((1.0_f64 - t3) - t4) + 0.0_f64)) / b);
            c = (t1 + t2);
            dc = ((t1 - c) + t2);
            y = (c + dc);
            retval = (-y);
            return retval;
        } else {
            y = (a + t2);
            retval = y;
            return retval;
        }
    }
    i = (256_f64.mul_add(ya, mfftnhf) as i32);
    z = ({
        z0 = (ya - xfg[(i) as usize][(0_i32) as usize]);
        z0
    } + yya);
    z2 = (z * z);
    pz = (z * z2).mul_add((z2).mul_add(e1, e0), z);
    fi = xfg[(i) as usize][(1_i32) as usize];
    gi = xfg[(i) as usize][(2_i32) as usize];
    if (n != 0) {
        t2 = ((pz * (fi + gi)) / (fi + pz));
        y = (gi - t2);
        retval = ((-sy) * y);
        return retval;
    } else {
        t2 = ((pz * (gi + fi)) / (gi - pz));
        y = (fi + t2);
        retval = (sy * y);
        return retval;
    }
}
