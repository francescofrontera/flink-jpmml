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
import org.apache.flink.ml.math.{DenseVector, SparseVector}
import org.apache.flink.ml.math.{Vector ⇒ FMLVector}
import shapeless._
import shapeless.labelled.FieldType

trait DerivableVector[CC] extends Serializable {
  def vector(in: CC): Vector[Double]
}

object DerivableVector {
  private[this] val eventFields = Set("modelid", "occurredon")

  private[this] def createDerivableVector[A](f: A ⇒ Vector[Double]): DerivableVector[A] = new DerivableVector[A] {
    override def vector(in: A): Vector[Double] = f(in)
  }

  def apply[A: DerivableVector]: DerivableVector[A] = implicitly[DerivableVector[A]]

  implicit def numberVector[T: Numeric]: DerivableVector[T] =
    createDerivableVector(v ⇒ Vector(implicitly[Numeric[T]].toDouble(v)))

  implicit val asString: DerivableVector[String] =
    createDerivableVector(strgValue ⇒ Vector(strgValue.toDouble))

  implicit val asDenseVector: DerivableVector[DenseVector] =
    createDerivableVector(_.toVector.map(_._2))

  implicit val asSparesVector: DerivableVector[SparseVector] =
    createDerivableVector(s ⇒ s.toVector.map(_._2))

  implicit val asVector: DerivableVector[Vector[Double]] =
    createDerivableVector(identity)

  implicit val asList: DerivableVector[List[Double]] =
    createDerivableVector(_.toVector)

  //FIXME: Consider to remove after avoid vector ML Usage
  implicit def vector: DerivableVector[FMLVector] = createDerivableVector {
    case v: DenseVector ⇒ asDenseVector.vector(v)
    case v: SparseVector ⇒ asSparesVector.vector(v)
  }

  implicit def nilVector: DerivableVector[HNil] = createDerivableVector(_ ⇒ Vector.empty[Double])

  implicit def cNilVector: DerivableVector[CNil] = createDerivableVector(_ ⇒ Vector.empty[Double])

  implicit def cValueVector[K <: Symbol, H, TL <: Coproduct](
      implicit keyWitness: Witness.Aux[K],
      heads: Lazy[DerivableVector[H]],
      tails: DerivableVector[TL]): DerivableVector[FieldType[K, H] :+: TL] =
    createDerivableVector {
      case Inl(head) ⇒ heads.value.vector(head)
      case Inr(tail) ⇒ tails.vector(tail)

    }

  implicit def valueVector[K <: Symbol, H, TL <: HList](
      implicit keyWitness: Witness.Aux[K],
      heads: Lazy[DerivableVector[H]],
      tails: DerivableVector[TL]): DerivableVector[FieldType[K, H] :: TL] =
    createDerivableVector {
      case _ :: tl if eventFields.contains(keyWitness.value.name.toLowerCase) => tails.vector(tl)
      case h :: tl ⇒ heads.value.vector(h) ++ tails.vector(tl)
    }

  implicit def ccToVector[CC, HL <: HList](
      implicit lGen: LabelledGeneric.Aux[CC, HL],
      dVector: DerivableVector[HL]
  ): DerivableVector[CC] = createDerivableVector(in ⇒ dVector.vector(lGen.to(in)))
}
