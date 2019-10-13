/*
 * Copyright (C) 2017  Radicalbit
 *
 * This file is part of flink-JPMML
 *
 * flink-JPMML is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * flink-JPMML is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with flink-JPMML.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.radicalbit.flink.pmml.scala.api.converter

import io.radicalbit.flink.pmml.scala.models.input.BaseEvent
import shapeless._
import shapeless.labelled.FieldType

trait DerivableVector[CC] extends Serializable {
  def vector(in: CC): Vector[Double]
}

object DerivableVector {
  def apply[A: DerivableVector]: DerivableVector[A] = implicitly[DerivableVector[A]]

  implicit def numberVector[T: Numeric]: DerivableVector[T] = new DerivableVector[T] {
    override def vector(in: T): Vector[Double] = {
      val asDouble: Double = implicitly[Numeric[T]].toDouble(in)
      Vector(asDouble)
    }
  }

  implicit val asString: DerivableVector[String] = new DerivableVector[String] {
    override def vector(in: String): Vector[Double] = Vector(in.toDouble)

  }

  implicit def nilVector[N <: HNil]: DerivableVector[N] = new DerivableVector[N] {
    override def vector(in: N): Vector[Double] = Vector.empty[Double]
  }

  implicit def valueVector[K <: Symbol, H, TL <: HList](
      implicit keyWitness: Witness.Aux[K],
      heads: Lazy[DerivableVector[H]],
      tails: DerivableVector[TL]): DerivableVector[FieldType[K, H] :: TL] =
    new DerivableVector[FieldType[K, H] :: TL] {
      override def vector(in: FieldType[K, H] :: TL): Vector[Double] =
        keyWitness.value.name.toLowerCase match {
          case "occurredon" | "modelid" ⇒ tails.vector(in.tail)
          case _ ⇒ heads.value.vector(in.head) ++ tails.vector(in.tail)
        }
    }

  //Enrich support for coProducts..

  implicit def ccToVector[CC <: BaseEvent, HL <: HList](
      implicit lGen: LabelledGeneric.Aux[CC, HL],
      dVector: DerivableVector[HL]
  ): DerivableVector[CC] = new DerivableVector[CC] {
    override def vector(in: CC): Vector[Double] = dVector.vector(lGen.to(in))
  }
}
