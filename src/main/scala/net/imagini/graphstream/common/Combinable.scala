package net.imagini.graphstream.common

/**
 * Created by mharis on 10/09/15.
 */

trait Combinable[A <: Combinable[A]] {
  def combine(other: A): A
}
