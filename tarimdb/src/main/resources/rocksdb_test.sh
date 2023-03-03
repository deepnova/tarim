#!/bin/bash

DB=target/db001

gen_data()
{
    for i in {100..999}
    do
        key="key$i"
        value="$v$i"
        ldb --db=${DB} put $key $value --create_if_missing
    done
}

gen_data2()
{
    batch=""
    for i in {101..1000}
    do
        key="key_$i"
        value="value_$i"
        batch="$batch $key $value"
        if [ $(expr $i % 100) -eq 0 ]; then
            echo "$i: $batch"
            ldb --db=${DB} batchput $batch --create_if_missing
            batch=""
        fi
    done
}

gen_data3()
{
    for a in {a..z} # too many key-value, stop at 'e' or 'f'
    do
        for b in {a..z}
        do
            for c in {a..z}
            do
                for d in {a..z}
                do
                    key="k_$a$b$c$d"
                    value="v_$a$b$c$d"
                    batch="$batch $key $value"
                done
            done
            echo "[---batch---] $batch"
            ldb --db=${DB} batchput $batch --create_if_missing
            batch=""
        done
    done
}

gen_data3
