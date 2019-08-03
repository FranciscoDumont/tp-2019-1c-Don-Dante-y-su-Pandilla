#!/bin/bash
cp ./configuraciones/kernel/knl01.cfg ../KNL/
cp ./configuraciones/kernel/lfs01.cfg ../LFS/
cp ./configuraciones/kernel/mem01.cfg ../MEM/
cp ./configuraciones/kernel/mem02.cfg ../MEM/
cp ./configuraciones/kernel/mem03.cfg ../MEM/
cp ./configuraciones/kernel/mem04.cfg ../MEM/
mkdir -p /home/utnso/lfs-prueba-kernel
cp ./configuraciones/kernel/Metadata.bin /home/utnso/lfs-prueba-kernel/
