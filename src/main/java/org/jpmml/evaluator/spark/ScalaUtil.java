/*
 * Copyright (c) 2015 Villu Ruusmann
 *
 * This file is part of JPMML-Evaluator
 *
 * JPMML-Evaluator is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JPMML-Evaluator is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with JPMML-Evaluator.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jpmml.evaluator.spark;

import java.util.Collections;
import java.util.List;

import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.Set;

public class ScalaUtil {

	private ScalaUtil(){
	}

	static
	public <E> Seq<E> emptySeq(){
		return toSeq(Collections.<E>emptyList());
	}

	static
	public <E> Seq<E> singletonSeq(E element){
		return toSeq(Collections.<E>singletonList(element));
	}

	static
	public <E> Seq<E> toSeq(List<E> list){
		return JavaConversions.asScalaBuffer(list);
	}

	static
	public <E> Set<E> singletonSet(E element){
		return new Set.Set1<>(element);
	}
}