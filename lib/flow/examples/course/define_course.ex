defmodule Ariadne.Flow.Examples.Course.DefineCourse do
  alias Ariadne.Flow.Composite
  alias Ariadne.Flow.Examples.Course.Events
  alias Ariadne.Flow.Examples.Course.Projections

  def command(%{course_id: course_id, capacity: capacity}) do
    Composite.new(
      %{
        exists?: Projections.course_exists(course_id)
      },
      fn
        %{exists?: true} -> {:error, :course_already_exists}
        _ -> {:ok, [%Events.CourseDefined{course_id: course_id, capacity: capacity}]}
      end
    )
  end
end
