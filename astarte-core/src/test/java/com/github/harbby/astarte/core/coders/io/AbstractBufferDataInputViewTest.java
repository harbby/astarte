/*
 * Copyright (C) 2018 The Astarte Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.harbby.astarte.core.coders.io;

import com.github.harbby.astarte.core.coders.Encoder;
import com.github.harbby.astarte.core.coders.EncoderChecker;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class AbstractBufferDataInputViewTest
{
    @Ignore
    @Test
    public void utf16CharCountTest()
    {
        Encoder<Integer> utf16CharCountEncoder = new Encoder<Integer>()
        {
            @Override
            public void encoder(Integer value, DataOutputView output)
            {
                DataOutputViewImpl outputView = (DataOutputViewImpl) output;
                outputView.writeUtf16CharCount(value);
            }

            @Override
            public Integer decoder(DataInputView input)
            {
                AbstractBufferDataInputView inputView = (AbstractBufferDataInputView) input;
                return inputView.readUtf16CharCount();
            }
        };
        EncoderChecker<Integer> checker = new EncoderChecker<>(utf16CharCountEncoder);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            byte[] bytes = checker.encoder(i);
            int rs = checker.decoder(bytes);
            Assert.assertEquals(i, rs);
        }
    }

    @Ignore
    @Test
    public void utf16CharCountTest2()
    {
        Encoder<Integer> utf16CharCountEncoder = new Encoder<Integer>()
        {
            @Override
            public void encoder(Integer value, DataOutputView output)
            {
                DataOutputViewImpl outputView = (DataOutputViewImpl) output;
                outputView.writeUtf16CharCount(value);
            }

            @Override
            public Integer decoder(DataInputView input)
            {
                AbstractBufferDataInputView inputView = (AbstractBufferDataInputView) input;
                return inputView.readUtf16CharCount();
            }
        };
        EncoderChecker<Integer> checker = new EncoderChecker<>(utf16CharCountEncoder);
        for (int i = Integer.MIN_VALUE; i < 0; i++) {
            byte[] bytes = checker.encoder(i);
            int rs = checker.decoder(bytes);
            Assert.assertEquals(i, rs);
        }
    }
}
