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
package $packageName;

import com.github.harbby.astarte.core.operator.CalcOperator;
import com.github.harbby.gadtry.base.Iterators;
import com.github.harbby.gadtry.collection.StateOption;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class $className<O>
        extends com.github.harbby.astarte.core.codegen.BaseCodegenIterator<O>
{
    private final StateOption<O> option = StateOption.empty();
    private final Iterator<?> iterator;
    private Iterator<O> child = Iterators.empty();

    // field mapping
    $fieldDefine

    public $className(Iterator<?> iterator, List<CalcOperator<?, ?>> operators)
    {
        super(iterator, operators);
        this.iterator = iterator;
        // field mapping
        $fieldCreate
    }

    @Override
    public boolean hasNext()
    {
        if (option.isDefined()) {
            return true;
        }
        if (child.hasNext()) {
            option.update(child.next());
            return true;
        }
        while (iterator.hasNext()) {
            Object value = iterator.next();
            // code gen...
            $calcCode
        }
        return false;
    }

    @Override
    public O next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return option.remove();
    }
}
