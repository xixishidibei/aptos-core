module 0x815::m {

    fun some_f(x: u64): u64 {
        x
    }

    fun t1(c: bool): bool {
        c
    }

    fun t2(c: bool): bool {
        if (c) false else true
    }

    fun t3(c: u64) {
        while (c > 0) c = some_f(c)
    }

    /*
    fun t4(c: u64) {
         while (c > 0) {
            if (c > 10) {
                c = some_f(c)
            }
         }
     }
     */

}
