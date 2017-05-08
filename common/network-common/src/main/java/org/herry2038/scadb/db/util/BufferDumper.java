//=========================================================================\\
//     _____               _ _
//    / ____|             | | |
//   | (___   ___ __ _  __| | |__
//    \___ \ / __/ _` |/ _` | '_ \
//    ____) | (_| (_| | (_| | |_) |
//   |_____/ \___\__,_|\__,_|_.__/

// Copyright 2016 The Scadb Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//=========================================================================\\
package org.herry2038.scadb.db.util;

import io.netty.buffer.ByteBuf;

public class BufferDumper {

    public static final String dumpAsHex(ByteBuf buffer) {
        int length = buffer.readableBytes();
        byte[] byteBuffer = new byte[length];

        buffer.markReaderIndex();
        buffer.readBytes(byteBuffer);
        buffer.resetReaderIndex();

        StringBuffer outputBuf = new StringBuffer(length * 4);

        int p = 0;
        int rows = length / 8;

        for (int i = 0; (i < rows) && (p < length); i++) {
            int ptemp = p;

            outputBuf.append(i + ": ");

            for (int j = 0; j < 8; j++) {
                String hexVal = Integer.toHexString(byteBuffer[ptemp] & 0xff);

                if (hexVal.length() == 1) {
                    hexVal = "0" + hexVal; //$NON-NLS-1$
                }

                outputBuf.append(hexVal + " "); //$NON-NLS-1$
                ptemp++;
            }

            outputBuf.append("    "); //$NON-NLS-1$

            for (int j = 0; j < 8; j++) {
                int b = 0xff & byteBuffer[p];

                if (b > 32 && b < 127) {
                    outputBuf.append((char) b + " "); //$NON-NLS-1$
                } else {
                    outputBuf.append(". "); //$NON-NLS-1$
                }

                p++;
            }

            outputBuf.append("\n"); //$NON-NLS-1$
        }

        outputBuf.append(rows + ": ");

        int n = 0;

        for (int i = p; i < length; i++) {
            String hexVal = Integer.toHexString(byteBuffer[i] & 0xff);

            if (hexVal.length() == 1) {
                hexVal = "0" + hexVal; //$NON-NLS-1$
            }

            outputBuf.append(hexVal + " "); //$NON-NLS-1$
            n++;
        }

        for (int i = n; i < 8; i++) {
            outputBuf.append("   "); //$NON-NLS-1$
        }

        outputBuf.append("    "); //$NON-NLS-1$

        for (int i = p; i < length; i++) {
            int b = 0xff & byteBuffer[i];

            if (b > 32 && b < 127) {
                outputBuf.append((char) b + " "); //$NON-NLS-1$
            } else {
                outputBuf.append(". "); //$NON-NLS-1$
            }
        }

        outputBuf.append("\n"); //$NON-NLS-1$
        outputBuf.append("Total " + byteBuffer.length + " bytes read\n");

        return outputBuf.toString();
    }

}