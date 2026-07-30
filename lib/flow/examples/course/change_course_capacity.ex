defmodule Ariadne.Flow.Examples.Course.ChangeCourseCapacity do
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Examples.Course.Events
  alias Ariadne.Flow.Examples.Course.Projections

  def command(%{course_id: course_id, new_capacity: new_capacity}) do
    Composite.new(
      %{
        course_exists?: Projections.course_exists(course_id),
        course_capacity: Projections.course_capacity(course_id)
      },
      fn
        %{course_exists?: false} ->
          {:error, :course_not_found}

        %{course_capacity: ^new_capacity} ->
          {:error, :no_change}

        _ ->
          {:ok, [%Events.CourseCapacityChanged{course_id: course_id, new_capacity: new_capacity}]}
      end
    )
  end
end
